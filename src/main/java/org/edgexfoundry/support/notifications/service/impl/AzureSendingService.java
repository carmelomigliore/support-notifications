package org.edgexfoundry.support.notifications.service.impl;

import com.microsoft.azure.sdk.iot.device.*;
import org.edgexfoundry.controller.AddressableClient;
import org.edgexfoundry.domain.meta.Addressable;
import org.edgexfoundry.support.domain.notifications.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;

@Service ("AzureSendingService")
public class AzureSendingService extends AbstractSendingService {

    private final static org.edgexfoundry.support.logging.client.EdgeXLogger logger = org.edgexfoundry.support.logging.client.EdgeXLoggerFactory
            .getEdgeXLogger(AzureSendingService.class);

    @Autowired
    private AddressableClient addressableClient;

    private static HashMap<String, AzureSender> senders = new HashMap<>();

    @Override
    TransmissionRecord sendToReceiver(Notification notification, Channel channel) {
        AzureChannel c = (AzureChannel) channel;
        AzureSender s = null;
        synchronized (senders) {
            s = senders.get(c.getAddressable());
            if (s == null) {
                Addressable addr = addressableClient.addressableForName(c.getAddressable());
                logger.info("creating MQTT client: " + addr.getAddress());
                s = new AzureSender(addr);
                senders.put(c.getAddressable(), s);
            }
        }
        synchronized (s) {
            TransmissionRecord record = new TransmissionRecord();
            try {
                record.setSent(System.currentTimeMillis());
                s.sendMessage(notification.getContent().getBytes());
                logger.info("Notification sent to Azure " + s.connectionString);
                record.setStatus(TransmissionStatus.SENT);
                record.setResponse("Notification sent to Azure");
            } catch (Exception e) {
                record.setStatus(TransmissionStatus.FAILED);
                record.setResponse(e.getMessage());
            }
            return record;
        }
    }

    private class AzureSender implements IotHubConnectionStateCallback, IotHubEventCallback {

        private final static String HOST_NAME = "HostName=";
        private final static String DEVICE_ID = ";DeviceId=";
        private final static String SHARED_ACCESS = ";SharedAccessKey=";
        private DeviceClient client;
        private StringBuffer connectionString;
        private boolean connected;

        @Override
        public void execute(IotHubConnectionState iotHubConnectionState, Object o) {
            switch (iotHubConnectionState) {
                case CONNECTION_SUCCESS:
                    connected = true;
                    break;

                case CONNECTION_DROP:
                case SAS_TOKEN_EXPIRED:
                    try {
                        client.closeNow();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    logger.debug("Shutting down...");
                    connected = false;
            }
        }

        @Override
        public void execute(IotHubStatusCode status, Object context) {
            logger.info("IoT Hub responded to message with status " + status.name());
            if (context != null) {
                synchronized (context) {
                    context.notify();
                }
            }
        }

        public AzureSender(Addressable addressable) {
            logger.debug("Creating Azure MQTT Sendor");
            this.connectionString = new StringBuffer(HOST_NAME);
            this.connectionString.append(addressable.getAddress());
            this.connectionString.append(DEVICE_ID);
            this.connectionString.append(addressable.getUser());
            this.connectionString.append(SHARED_ACCESS);
            this.connectionString.append(addressable.getPassword());
            logger.debug("Starting IoT Hub distro...");
            logger.debug("Beginning IoT Hub setup.");
            logger.debug("Preparing to send to: " + this.connectionString);
            logger.debug("Azure connect with:  " + connectionString);
            try {
                client = new DeviceClient(connectionString.toString(), IotHubClientProtocol.AMQPS_WS);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
            client.registerConnectionStateCallback(this, null);
            logger.debug("Successfully created an IoT Hub client.");
        }

        public synchronized boolean sendMessage(byte[] messagePayload) {

            try {
                //if(!connected) {
                client.open();
                logger.debug("Opened connection to IoT Hub.");
                //	}
                Message msg = new Message(messagePayload);
                msg.setExpiryTime(5000);
                //Object lockobj = new Object();

                client.sendEventAsync(msg, this, null);
			/*synchronized (lockobj) {
				lockobj.wait();
			}*/
                return true;
            } catch (Exception e) {
                logger.error("Failure: " + e.toString());
            }
            return false;
        }
    }
}
