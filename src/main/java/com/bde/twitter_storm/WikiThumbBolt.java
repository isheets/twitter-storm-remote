package com.bde.twitter_storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.JSONException;
import twitter4j.JSONObject;

/*
 * Find the link to the wiki thumbnail, if exists
 */

public class WikiThumbBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String wikiURL = (String) input.getValueByField("wikiURL");
        String thumbLink = getThumbLink(wikiURL);
        if (thumbLink != null) {
            System.out.println("Entity: " + input.getValueByField("entityName"));
            
            collector.emit(new Values(
                input.getValueByField("entityName"),
                input.getValueByField("wikiURL"),
                input.getValueByField("tweetID"),
                input.getValueByField("tweetCreatedAt"),
                thumbLink
            ));
            
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entityName", "wikiURL", "tweetID", "tweetCreatedAt", "wikiImageURL"));
    }

    public String getThumbLink(String urlString) {
        try {
            String[] tokens = urlString.split("/");
            String title = tokens[tokens.length - 1];
            String imagesURL = "https://en.wikipedia.org/w/api.php?action=query&titles=" + title + "&prop=pageimages&format=json&pithumbsize=300";

            URL url = new URL(imagesURL);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("Content-Type", "jsonp");
            con.setConnectTimeout(5000);
            con.setReadTimeout(5000);

            int status = con.getResponseCode();
            BufferedReader in = null;

            if (status > 299) {
                in = new BufferedReader(new InputStreamReader(con.getErrorStream()));
            } else {
                in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            }
            String inputLine;
            StringBuffer response = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            con.disconnect();
            JSONObject jsonRes = new JSONObject(response.toString());
            JSONObject page = jsonRes.getJSONObject("query").getJSONObject("pages");
            String id = page.keys().next();
            String imageURL = page.getJSONObject(id).getJSONObject("thumbnail").getString("source");

            // emit
            return imageURL;

        } catch (MalformedURLException e) {
            System.err.println("BADURL");
        } catch (IOException e) {
            System.err.println("Cant open connection");
        } catch (JSONException e) {
            System.err.println("Invalid JSON!");
        }
        return null;
    }

}
