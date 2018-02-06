package org.edgexfoundry.support.notifications.service.impl;

import org.eclipse.paho.client.mqttv3.*;
import org.edgexfoundry.controller.AddressableClient;
import org.edgexfoundry.domain.meta.Addressable;
import org.edgexfoundry.support.domain.notifications.*;
import org.springframework.beans.factory.annotation.Autowired;

import javax.net.ssl.SSLContext;
import java.util.HashMap;

public class MQTTSendingService extends  AbstractSendingService{

    private final static org.edgexfoundry.support.logging.client.EdgeXLogger logger = org.edgexfoundry.support.logging.client.EdgeXLoggerFactory
            .getEdgeXLogger(MQTTSendingService.class);

    @Autowired
    private AddressableClient addressableClient;

    private HashMap <String, MQTTSender> senders = new HashMap<>();

    @Override
    TransmissionRecord sendToReceiver(Notification notification, Channel channel) {
        MQTTChannel mqttChannel = (MQTTChannel)channel;
        MQTTSender s = senders.get(mqttChannel.getAddressable());
        if(s == null){
            Addressable addr = addressableClient.addressableForName(mqttChannel.getAddressable());
            s = new MQTTSender(addr);
            senders.put(mqttChannel.getAddressable(), s);
        }
        TransmissionRecord record = new TransmissionRecord();
        try {
            record.setSent(System.currentTimeMillis());
            s.sendMessage(notification.getContent().getBytes());
            record.setStatus(TransmissionStatus.SENT);
            record.setResponse("Notification sent to MQTT broker");
        } catch (Exception e){
            record.setStatus(TransmissionStatus.FAILED);
            record.setResponse(e.getMessage());
        }
        return record;
    }

    private static String buildBrokerUrl(Addressable addressable) {
        return addressable.getProtocol().toString().toLowerCase() + "://" + addressable.getAddress();
    }

    private class MQTTSender implements MqttCallback {

        // private final static Logger logger = Logger.getLogger(MQTTSender.class);
        // replace above logger with EdgeXLogger below


        private MqttClient client = null;

        private String brokerUrl;
        private int brokerPort;
        private String clientId;
        private String user;
        private String password;
        private String topic;
        private int qos;
        private int keepAlive;
        private boolean secureConnection;

        public MQTTSender(String brokerUrl, int brokerPort, String clientId, String user, String password, String topic,
                          int qos, int keepAlive) {
            this.brokerUrl = brokerUrl;
            this.brokerPort = brokerPort;
            this.clientId = clientId;
            this.user = user;
            this.password = password;
            this.topic = topic;
            this.qos = qos;
            this.keepAlive = keepAlive;
            this.secureConnection = brokerUrl.startsWith("ssl:");
            this.connectClient();
        }

        public MQTTSender(Addressable addressable) {
            this(buildBrokerUrl(addressable), addressable.getPort(), addressable.getPublisher(), addressable.getUser(),
                    addressable.getPassword(), addressable.getTopic(), 0, 3600);
        }



        public boolean sendMessage(byte[] messagePayload) throws Exception {
            if (client != null) {
                try {
                    MqttMessage message = new MqttMessage(messagePayload);
                    message.setQos(qos);
                    message.setRetained(false);
                    client.publish(topic, message);
                    return true;
                } catch (Exception e) {
                    logger.error("Failed to send outbound message to topic:  " + topic + " - unexpected issue: "
                            + new String(messagePayload));
                    throw (e);
                }
            }
            return false;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getTopic() {
            return topic;
        }

        private void connectClient() {
            try {
                client = new MqttClient(brokerUrl + ":" + brokerPort, clientId);
                client.setCallback(this);
                MqttConnectOptions connOpts = new MqttConnectOptions();
                connOpts.setUserName(user);
                connOpts.setPassword(password.toCharArray());
                connOpts.setCleanSession(true);
                connOpts.setKeepAliveInterval(keepAlive);
                if (secureConnection) {
                    try {
                        EdgeXSecurityManager secMgr = new EdgeXSecurityManager();
                        SSLContext ctxt = SSLContext.getInstance("TLS");
                        ctxt.init(secMgr.getKeyManagers(), secMgr.getTrustManagers(), new java.security.SecureRandom());
                        connOpts.setSocketFactory(ctxt.getSocketFactory());
                    } catch (Exception e) {
                        logger.error("Failed to initialize secure connection to broker.", e);
                        throw new MqttSecurityException(e);
                    }
                }
                logger.debug("Connecting to broker:  " + brokerUrl);
                client.connect(connOpts);
                logger.debug("Connected");
            } catch (MqttException e) {
                logger.error("Failed to connect to MQTT client ( " + brokerUrl + ":" + brokerPort + "/" + clientId
                        + ") for outbound messages");
                e.printStackTrace();
            }
        }

        public void closeClient() {
            try {
                if (client != null) {
                    client.disconnect();
                    client.close();
                }
            } catch (MqttException e) {
                logger.error("Problems disconnecting and closing the client.");
                e.printStackTrace();
            }
        }

        @Override
        public void connectionLost(Throwable cause) {
            logger.error("Outgoing sendor publisher connection lost for topic " + topic + " - issue:"
                    + cause.getLocalizedMessage());
            try {
                client.close();
            } catch (MqttException e) {
                logger.error("Unable to close the client.");
                e.printStackTrace();
            }
            connectClient();
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
            logger.error("Message received on Outgoing Sender for topic: " + topic
                    + ", which should not happen.  Payload:  " + message.getPayload().toString());
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            logger.debug("Message delivered successfully by Outgoing Sender to topic:  " + topic + ".  Token:  "
                    + token.toString());
        }
    }
}
