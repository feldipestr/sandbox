  ### TL;DR;    
    
- Streaming pipeline provides online statistics of the [Dota 2](https://store.steampowered.com/app/570/Dota_2/) live matches (games) to the registered spectators    
 - Technological stack:     
    - *Events queue:* [Google Cloud PubSub](https://cloud.google.com/pubsub)    
    - *Events data processing engine*: [Google Cloud Dataflow](https://cloud.google.com/dataflow)    
    - *Data processing framework*: [Apache Beam](https://beam.apache.org/)   
    - *Key-value store*: [Google Cloud Datastore](https://cloud.google.com/datastore)    
    - *REST API*: [Google Cloud Functions](https://cloud.google.com/functions)    
    - *Live Dota 2 games data provider*: [Steam REST API](https://partner.steamgames.com/doc/webapi_overview)     
    - *Events listener*: JavaScript    
    - *Language*: Python 3    
    
# Dota 2 Game State Streaming 

The service allows to spectators (e.g. commentators or Dota 2 matches broadcasters) retrieve game events data from an online match watched inside Dota 2 application and provide live statistics about team is in the lead, player names, heroes, gold, kills, items and e.t.c.  
  
As the service is scalable it allows to collect and provide statistics for many matches (games) and spectators. Spectators get their personal unique keys (tokens) and request with required frequency current state (statistic) for a match which they watch at the moment.   
  
There is only one token available to be registered per one Dota 2 application. Spectators create configuration files in the special folder of the application and Dota 2 application begin emit all the events if the spectator connects to a live match. Each event contains the token which helps to aggregate events for a particular spectator and match (game) in the service.   

## Service configuration 
   
### Dota 2 application configuration to launch game state coordinator 

Spectators have to have [Dota 2 application](https://store.steampowered.com/app/570/Dota_2/) installed. Then, special configuration file has to be created in the application folder as stated [here](https://www.npmjs.com/package/dota2-gsi#configuring-the-dota-2-client.).  
   
### Launch of events listener 

There is a url to an events-listener in the configuration file. The Dota 2 application sends all of the events (actions) which happen in the live watching match (game) to the events-listener.   
  
The listener has to be installed on a public server (e.g. [Compute Engine or Kubernetes Engine](https://cloud.google.com/compute/docs/containers/deploying-containers)) with the Docker image which is built from `./events_listener/Dockerfile` file.  
  
The events-listener is a JavaScript microservice which was developed by  [xzion/dota2-gsi](https://www.npmjs.com/package/dota2-gsi).    
  
The listener sends all of the events to the Google PubSub queue:    
 - *topic*: **dota2-gsi-queue**      
 - *subscription*: **dota2-gsi-queue-subscription**  
  
Events from all of the spectators are sent to the same queue and distributed between tokens.  
  
### Launch of events streaming processor  
  
Before events processor is launched create 2 folders in the Google Cloud Bucket:  
 - `gs://{project_id}/dota2-gsi-streaming/binaries` for the Dataflow's job staging files location  
 - `gs://{project_id}/dota2-gsi-streaming/tmp` for the Dataflow's job temporary files location  
      
Events streaming processor uses [Steam REST API](https://partner.steamgames.com/doc/webapi_overview) to retrieve players' team names.   
Steam REST API requires to obtain [Steam REST API key](https://partner.steamgames.com/doc/webapi_overview/auth#user-keys) to be able to run requests to the API.   
Steam REST API string key(s) should be stored in `./lib/steam_api_keys.txt` file.      
      
Steam REST API has [requests limit](https://steamcommunity.com/dev/apiterms):        
        
```You are limited to one hundred thousand (100,000) calls to the Steam Web API per day. Valve may approve higher daily call limits if you adhere to these API Terms of Use.```   

So, it is better to have a couple or more of the keys in the `steam_api_keys.txt` file (1 key per 1 row) if you are going to have high-loaded streaming service.      
      
Events processor itself can be launched as Google Dataflow streaming job via Python script:  
```bash 
python3 ./events_processor/dota2_gsi_events_streamer.py --setup_file ./events_processor/setup.py      
 ``` 
    
### Launch of matches statistics REST API  

Matches statistics REST API is launched by [Google Cloud Functions](https://cloud.google.com/functions/docs/quickstart-python).   
Copy the function's code from `./statistics_rest_api/function-game-stat.py` file and dependencies requirements from `./statistics_rest_api/requirements.txt` file to the new Google Cloud HTTP Function.      
Sending the token value as a parameter into the Google Cloud Function url spectators can receive in response online statistics in JSON format for the watching match.