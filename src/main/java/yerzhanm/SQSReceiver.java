package yerzhanm;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SQSReceiver extends Receiver<String> {

    private String sqsQueueUrl;
    private String queuename;
    private Regions awsRegion = Regions.DEFAULT_REGION;
    private Map<String, String> credentials;


    public SQSReceiver(String queuename) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.queuename = queuename;
    }

    @Override
    public void onStart() {
        new Thread()  {
            @Override public void run() {
                receive();
            }
        }.start();
    }

    @Override
    public void onStop() {
        // There is nothing much to do as the thread calling poll()
        // is designed to stop by itself isStopped() returns false
    }

    private void receive(){
        AmazonSQS sqs = null;

        if(credentials!=null){
            sqs = new AmazonSQSClient(new BasicAWSCredentials(credentials.get("accessKeyId"), credentials.get("secretAccessKey")));
        }else {
            sqs = new AmazonSQSClient(new DefaultAWSCredentialsProviderChain());
        }

        sqs.setRegion(Region.getRegion(awsRegion));
        sqsQueueUrl = sqs.getQueueUrl(queuename).getQueueUrl();
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsQueueUrl);
        try {
            while (!isStopped()) {
                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
                for (Message message : messages) {
                    store(message.getBody());
                    sqs.deleteMessage(new DeleteMessageRequest(sqsQueueUrl, message.getReceiptHandle()));
                }
            }
            restart("Trying to connect again");
        }catch(Throwable t) {
            // restart if there is any other error
            restart("Error receiving data", t);
        }
    }

    public SQSReceiver at(Regions awsRegion) {
        this.awsRegion = awsRegion;
        return this;
    }

    public SQSReceiver withCredentials(String accessKeyId, String secretAccessKey){
        this.credentials = new HashMap<>();
        this.credentials.put("accessKeyId", accessKeyId);
        this.credentials.put("secretAccessKey", secretAccessKey);
        return this;
    }

    public SQSReceiver withCredentials(String filename) throws IOException {
        AWSCredentials propertiesCredentials = new PropertiesCredentials(new File(filename));
        this.credentials.put("accessKeyId", propertiesCredentials.getAWSAccessKeyId());
        this.credentials.put("secretAccessKey", propertiesCredentials.getAWSSecretKey());
        return this;
    }
}
