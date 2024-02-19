package eu.h2020.symbiote.rappluginexample;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import com.google.gson.JsonObject;
import eu.h2020.symbiote.model.cim.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.WaitForPort;
import eu.h2020.symbiote.cloud.model.rap.ResourceInfo;
import eu.h2020.symbiote.cloud.model.rap.query.Query;
import eu.h2020.symbiote.rapplugin.messaging.rap.ActuatorAccessListener;
import eu.h2020.symbiote.rapplugin.messaging.rap.RapPlugin;
import eu.h2020.symbiote.rapplugin.messaging.rap.RapPluginException;
import eu.h2020.symbiote.rapplugin.messaging.rap.ResourceAccessListener;
import eu.h2020.symbiote.rapplugin.messaging.rap.ServiceAccessListener;
import eu.h2020.symbiote.rapplugin.messaging.rap.SubscriptionListener;
import eu.h2020.symbiote.rapplugin.value.Value;
import org.springframework.http.HttpStatus;

@SpringBootApplication
public class RapPluginExampleApplication implements CommandLineRunner {
    public static final Logger LOG = LoggerFactory.getLogger(RapPluginExampleApplication.class);

    @Autowired
    RapPlugin rapPlugin;

    private ObjectMapper mapper = new ObjectMapper();


    public String UNDEFINED = "undefined";

    public static void main(String[] args) {
        WaitForPort.waitForServices(WaitForPort.findProperty("SPRING_BOOT_WAIT_FOR_SERVICES"));

        System.out.println("########## Starting RAP plugin microservice #########");
        System.out.println("########## Version 06-09-2021 #########");
        SpringApplication.run(RapPluginExampleApplication.class, args);
    }//end
//----------------------------------------------------------------------------------------


    //----------------------------------------------------------------------------------------
    @Override
    public void run(String... args) throws Exception {
        registerListeners();

    }//end
    //----------------------------------------------------------------------------------------
    private void registerListeners() {

        System.out.println("####### Register Listeners ########################");

        rapPlugin.registerReadingResourceListener(new ResourceAccessListener() {

            @Override
            public String getResourceHistory(List<ResourceInfo> resourceInfo, int top, Query filterQuery) {
                LOG.debug("reading resource history with info {}", resourceInfo);

                String startDate = UNDEFINED;
                String endDate = UNDEFINED;

                /*
                 * Check if the resourceInfo
                 * has information about the
                 * start and end of historical data.
                 */
                System.out.println("size = " + resourceInfo.size());
                if (resourceInfo.size() == 4) {
                    startDate = resourceInfo.get(2).getInternalId();
                    endDate = resourceInfo.get(3).getInternalId();
                }
                if (resourceInfo.size() == 3) {
                    startDate = resourceInfo.get(1).getInternalId();
                    endDate = resourceInfo.get(2).getInternalId();
                }

                System.out.println("********************************************************************");
                System.out.println("Requesting observation from : " + startDate + " to: " + endDate);
                System.out.println("********************************************************************");

                /*
                 * Get the internalId of resource
                 */

                String resourceId = resourceInfo.get(0).getInternalId();//Utils.getInternalResourceId(resourceInfo);
                LOG.debug("resourceId found {}", resourceId);
                LOG.debug("top =  {}", top);

                try {
                    if ("isen2".equals(resourceId) || "isen1".equals(resourceId)) {
                        // This is the place to put reading history data of sensor.
                        List<Observation> observations = new LinkedList<>();
                        top = 5;//just for test
                        for (int i = 0; i < top; i++) {
                            /*
                             * Add here the start and end timestamp criteria
                             */
                            observations.add(createObservation(resourceId));
                        }

                        try {
                            return mapper.writeValueAsString(observations);
                        } catch (JsonProcessingException e) {
                            System.out.println("Exception: Can not convert observations to JSON");
                            throw new RapPluginException(500, "Can not convert observations to JSON", e);
                        }
                    } else {
                        System.out.println("Exception: Sensor not found.");
                        throw new RapPluginException(404, "Sensor not found,supported only isen1,isen2.");
                    }
                } catch (Exception exc) {
                    throw new RapPluginException(HttpStatus.NOT_FOUND.value(), "Parking sensor(s) not found.");
                }



            }//end
            //-------------------------------------------------------------------------------------------------------        @Override
            public String getResource(List<ResourceInfo> resourceInfo) {
                String errorCode = "READING_RESOURCE_INFO";
                //Note: we assume that we have only one ResourceInfo in the list
                //Check when there are more than one ResourceInfo in the list

                System.out.println("getResource()");

                String startDate = UNDEFINED;
                String endDate = UNDEFINED;

                /*
                 * Check if the resourceInfo
                 * has information about the
                 * start and end of historical data.
                 */
                System.out.println("size = " + resourceInfo.size());
                if (resourceInfo.size() == 4) {
                    startDate = resourceInfo.get(2).getInternalId();
                    endDate = resourceInfo.get(3).getInternalId();
                }
                if (resourceInfo.size() == 3) {
                    startDate = resourceInfo.get(1).getInternalId();
                    endDate = resourceInfo.get(2).getInternalId();
                }

                System.out.println("********************************************************************");
                System.out.println("Requesting observation from : " + startDate + " to: " + endDate);
                System.out.println("********************************************************************");

                /*
                 * Get the internalId of resource
                 */

                String resourceId = resourceInfo.get(0).getInternalId();
                LOG.debug("resourceId found {}", resourceId);

                if ("isen2".equals(resourceId) || "isen1".equals(resourceId)) {
                    // This is the place to put reading data of sensor.
                    List<Observation> observations = new LinkedList<>();

                    /*
                     * Add here the start and end timestamp criteria
                     */
                    observations.add(createObservation(resourceId));


                    try {
                        return mapper.writeValueAsString(observations);
                    } catch (JsonProcessingException e) {
                        System.out.println("Exception: Can not convert observations to JSON");
                        throw new RapPluginException(500, "Can not convert observations to JSON", e);
                    }
                } else {
                    System.out.println("Exception: Sensor not found.");
                    throw new RapPluginException(404, "Sensor not found,supported only isen1 and isen2.");
                }

            }//end
        });
//-----------------------------------------------------------------------------------------
        /*
         * Note in this demo we use as internal id of the light Actuator
         * the lightInternalID1 or lightInternalID2.
         *
         * In the demo we have registered an Actuator
         * with one capability named "OnOffCapability"
         * with one  boolean parameter named "on".
         */
        rapPlugin.registerActuatingResourceListener(new ActuatorAccessListener() {

            @Override
            public void actuateResource(String internalId, Map<String, Map<String, Value>> capabilities) {
                System.out.println("Called actuation for resource " + internalId);

                if (("internalId".equals(internalId) == true )) {
                    System.out.println("Internal id " + internalId + " is supported");
                }else{
                    System.out.println("Internal id " + internalId + " is not supported");
                    throw new RapPluginException(404, "Internal id " + internalId + " not supported");
                }
                /*
                 * This is place to put actuation code
                 *  for resource with internal id = internalId
                 */
                if ("internalId".equals(internalId)) {//no need this extra checking !!!!
                    System.out.println(internalId + " is actuated");
                } else {
                    throw new RapPluginException(404, "Internal id " + internalId + " not supported");
                }
            }
        });

        rapPlugin.registerInvokingServiceListener(new ServiceAccessListener() {

            @Override
            public String invokeService(String internalId, Map<String, Value> parameters) {
                System.out.println("In invoking service of resource " + internalId);

                // print parameters
                for (Entry<String, Value> parameterEntry : parameters.entrySet()) {
                    System.out.println(" Parameter - name: " + parameterEntry.getKey() + " value: " +
                            parameterEntry.getValue().asPrimitive().asString());
                }

                try {
                    if ("rp_isrid1".equals(internalId)) {
                        return mapper.writeValueAsString("ok");
                    } else if ("isrid1".equals(internalId)) {
                        return mapper.writeValueAsString("some json");
                    } else {
                        throw new RapPluginException(404, "Service not found.");
                    }
                } catch (JsonProcessingException e) {
                    throw new RapPluginException(500, "Can not convert service response to JSON", e);
                }
            }
        });

        rapPlugin.registerNotificationResourceListener(new SubscriptionListener() {

            private Set<String> subscriptionSet = Collections.synchronizedSet(new HashSet<>());
            private volatile Thread subscriptionThread;

            @Override
            public void unsubscribeResource(String unSubscriptionConfiguration) {
                LOG.info("unSubscriptionConfiguration ", unSubscriptionConfiguration);


                String internalId,productId,sessionId;
                /*
                 * For iotfeds project the resourceId exists
                 * in a json message.
                 */

                if(!unSubscriptionConfiguration.contains("{") ) {
                    System.out.println("unSubscriptionConfiguration is not a json message, only json messages are supported");
                    /*
                     * just to keep compatibility
                     * for non json messages.
                     */
                    // resourceId = unSubscriptionConfiguration;
                    return;
                }else {
                    try {
                        JSONObject jsonMessage = new JSONObject(unSubscriptionConfiguration);
                        internalId = jsonMessage.getString("internalId");
                        productId  = jsonMessage.getString("productId");
                        sessionId  = jsonMessage.getString("sessionId");
                    } catch (Exception ex) {
                        System.out.println("Failed to parse the unSubscriptionConfiguration");
                        return;
                    }

                    System.out.println("internalId = " + internalId);
                    System.out.println("productId = " + productId);
                    System.out.println("sessionId = " + sessionId);
                }

                synchronized (subscriptionSet) {
                    subscriptionSet.remove(internalId);
                }

            }//end
            //------------------------------------------------------------------------------------------
            private void sendPush() {
                LOG.info("Sending notifications!!!!");
                synchronized (subscriptionSet) {
                    for (String id : subscriptionSet) {
                        Observation observation = createObservation(id);
                        rapPlugin.sendNotification(observation);
                        LOG.info("Notification for resource {}: {}", id, observation);

                    }
                }
            }//end
            //------------------------------------------------------------------------------------------
            @Override
            public void subscribeResource(String subscriptionConfiguration) {
                LOG.info("subscribeResource subscriptionConfiguration", subscriptionConfiguration);

                /*
                 * For iotfeds project the resourceId exists
                 * in a json message.
                 */
                String internalId,productId,sessionId,authenticationToken,frequency;

                if(!subscriptionConfiguration.contains("{") ) {
                    System.out.println("subscriptionConfiguration is not a json message, only json messages are supported");
                    // resourceId = subscriptionConfiguration;
                    //System.out.println("not json resourceId=  "+ resourceId);
                    return;
                }else {
                    try {
                        JSONObject jsonMessage = new JSONObject(subscriptionConfiguration);
                        internalId = jsonMessage.getString("internalId");
                        productId  = jsonMessage.getString("productId");
                        sessionId  = jsonMessage.getString("sessionId");
                        authenticationToken = jsonMessage.getString("authenticationToken");
                        frequency = jsonMessage.getString("frequency");
                    } catch (Exception ex) {
                        System.out.println("Failed to parse the subscriptionConfiguration");
                        return;
                    }

                    System.out.println("internalId = " + internalId);
                    System.out.println("productId = " + productId);
                    System.out.println("sessionId = " + sessionId);
                    System.out.println("authenticationToken = " + authenticationToken);
                    System.out.println("frequency = " + frequency);
                }

                synchronized (subscriptionSet) {

                    /*
                     * Add to subscription List the new
                     * resource Id.
                     */

                    subscriptionSet.add(internalId);

                    if (subscriptionThread == null) {

                        subscriptionThread = new Thread(() -> {

                            while (!subscriptionSet.isEmpty()) {
                                sendPush();
                                try {
                                    Thread.sleep(5000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            subscriptionThread = null;
                        });
                        subscriptionThread.start();
                    }
                }
            }//end


        });

    }//registerListeners
    //----------------------------------------------------------------------------------------------------------------------------------------------
    public Observation createObservation(String sensorId) {
        Location loc = new WGS84Location(48.2088475, 16.3734492, 158, "Stephansdome", Arrays.asList("City of Wien"));

        TimeZone zoneUTC = TimeZone.getTimeZone("UTC");
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        dateFormat.setTimeZone(zoneUTC);
        Date date = new Date();
        String timestamp = dateFormat.format(date);

        long ms = date.getTime() - 1000;
        date.setTime(ms);
        String samplet = dateFormat.format(date);

        int temperature   = getRandomNumber(30, 40);
        String tempString = Integer.toString(temperature);

        ObservationValue obsval = new ObservationValue(
                tempString,
                new Property("Temperature", "TempIRI", Arrays.asList("Air temperature")),
                new UnitOfMeasurement("C", "degree Celsius", "C_IRI", null));
        ArrayList<ObservationValue> obsList = new ArrayList<>();
        obsList.add(obsval);


        Observation obs = new Observation(sensorId, loc, timestamp, samplet, obsList);

        try {
            LOG.debug("Observation: \n{}", new ObjectMapper().writeValueAsString(obs));
        } catch (JsonProcessingException e) {
            LOG.error("Can not convert observation to JSON", e);
        }

        return obs;
    }//end
    //--------------------------------------------------------------------------------------------
    public int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }//end



}//end of class
