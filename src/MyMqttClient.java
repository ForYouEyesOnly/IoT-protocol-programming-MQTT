import org.eclipse.paho.client.mqttv3.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 *  This class implements a mqtt client which can subscribe and publish
 *  messages from or to a broker by using the java paho.client library.
 *  In this class, I also complete the task in Q3, which collect the received
 *  messages, loss rate, out-of-order rate and duplicate rate each in 1 min
 *  and publish some of them every 10 minutes.
 *  Authorship: Wenjun Yang u6251843
 *  Reference: https://gist.github.com/m2mIO-gister/5275324
 *
 */

public class MyMqttClient implements MqttCallback {

    //an ArrayList to keep all the received messages    a HasSet to keep all the received messages which are not the same
    ArrayList<Integer> messageList = new ArrayList<>(); HashSet<Integer> messageSet = new HashSet<>();
    //an ArrayList to keep the number of messages received in 1 min  an ArrayList to keep the number of duplicate messages
    ArrayList<Integer> messageRecv = new ArrayList<>(); ArrayList<Integer> messageDupe = new ArrayList<>();
    //an ArrayList to keep the number of lost messages  an ArrayList to keep the number of out-of-order messages
    ArrayList<Integer> messageLoss = new ArrayList<>(); ArrayList<Integer> messageOoo = new ArrayList<>();
    //keep the number of loss rate                  keep the number of duplicate rate
    ArrayList<Double> lossrate = new ArrayList<>(); ArrayList<Double> duperate = new ArrayList<>();
    //keep the number of out-of-order rate
    ArrayList<Double> ooorate = new ArrayList<>();

    /*!!!!!!!!!!! if you want to modify the subscribe QoS and subscribe topic, please change the value of following two variables !!!!!!!!!!! */
    //current subscribe QoS
    int subqos = 2;

    //current subscribe topic
    String subTopic = "counter/fast/q2";


    //the mqtt client
    MqttClient client;
    //the mqtt connection options used for connecting with the broker
    MqttConnectOptions myConnectOpt;


    /**
     * The connectionLost function will be called when the client
     * loses the connection to the broker.
     *
     */
    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("Connection to MQTT server lost");
    }

    /**
     * The messageArrived function will be called when the client has received
     * the messages from the broker (a specific topic) successfully.
     */
    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        /*System.out.println("Message received: "+ new String(mqttMessage.getPayload())+"\nQos: "+mqttMessage.getQos() + ", retain: "+mqttMessage.isRetained()
                            + ", dup: "+mqttMessage.isDuplicate()+"\n\n");*/

        if (isInt(new String(mqttMessage.getPayload()))) {
            messageList.add(Integer.valueOf(new String(mqttMessage.getPayload())));
            messageSet.add(Integer.valueOf(new String(mqttMessage.getPayload())));
        }
    }

    /**
     * The deliveryComplete function will be called when the client has published
     * the messages to the broker (a specific topic) successfully.
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken Token) {

    }


    /**
     * The run function is designed to complete the task of connecting
     * to the broker, publishing messages or subscribing to a specific
     * topic. It can record the data required in the Q3 in an arrayList
     * and publish some important data to the broker as well.
     *
     */

    public void run() {
        //the clientid
        String clientID = "3310-u6251843";
        //the broker url
        String serverURL = "tcp://3310exp.hopto.org";

        //set up a mqtt client, control how the client connects to a server
        myConnectOpt = new MqttConnectOptions();

        //set whether the client and server should remember state across restarts and reconnects.
        myConnectOpt.setCleanSession(true);

        // set the "keep alive" interval.
        myConnectOpt.setKeepAliveInterval(30);

        // set the username
        myConnectOpt.setUserName("3310student");

        //set the password
        myConnectOpt.setPassword("comp3310".toCharArray());


        try {
            //connect to a specific broker
            client = new MqttClient(serverURL, clientID);
            //set the callback listener to use for events that happen asynchronously
            client.setCallback(this);
            client.connect(myConnectOpt);
        } catch (MqttException e) {
            e.printStackTrace();
        }

        //print out if the client has connected to the broker
        System.out.println("Connected to " + serverURL);

        try {
            //subscribe to a specific topic with a QoS
            client.subscribe(subTopic, subqos);
        } catch (MqttException e) {
            e.printStackTrace();
        }

        //counter to record the index of an arrayList which the last number in last round stored
        int counter = 0; int recorder = 0;

        //publish messages every 10 mins
        for (int i = 0; i < 10; i++) {
            try {
                // 1 min for each round
                Thread.sleep(60000);
            } catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println("Round " + (i+1));
            //record the number of received messages in an arrayList
            if (i == 0) {
                messageRecv.add(0, messageList.size());
            } else {
                messageRecv.add(i, messageList.size() - sumList(messageRecv, i));
            }
            System.out.println("Messages received in " + (i+1) + " minute: " + messageRecv.get(i));

            //record the number of duplicate messages in an arrayList
            if (i == 0) {
                messageDupe.add(0, messageList.size() - messageSet.size());
            } else {
                messageDupe.add(i, messageList.size() - messageSet.size() - sumList(messageDupe, i));
            }
            System.out.println("Messages duplicate in " + (i+1) + " minute: " + messageDupe.get(i));

            //record the number of out-of-order messages in an arrayList
            if (i == 0) {
                messageOoo.add(0, checkOrder(messageList, 0, messageList.size() - 1));
                counter = messageList.size();
            } else {
                messageOoo.add(i, checkOrder(messageList, counter, messageList.size() - 1));
                counter = messageList.size();
            }
            System.out.println("Messages out-of-order in " + (i+1) + " minute: " + messageOoo.get(i));

            //record the number of lost messages in an arrayList
            if (i == 0) {
                messageLoss.add(0, messageList.get(messageList.size() - 1) - messageList.get(0) + 1 + messageDupe.get(0) - messageRecv.get(0));
                recorder = messageList.size();
            } else {
                messageLoss.add(i, messageList.get(messageList.size() - 1) - messageList.get(recorder) + 1 + messageDupe.get(i) - messageRecv.get(i));
                recorder = messageList.size();
            }
            System.out.println("Messages lost in " + (i+1) + " minute: " + messageLoss.get(i));
            System.out.println();

            if (i == 9) {

                //calculate the corresponding loss rate
                for (int k = 0; k < messageRecv.size(); k++) {
                    lossrate.add(k, ( messageLoss.get(k) / (double)(messageRecv.get(k) + messageLoss.get(k))));
                }
                //System.out.println(lossrate);

                //calculate the corresponding out-of-order rate
                for (int k = 0; k < messageRecv.size(); k++) {
                    ooorate.add(k, (messageOoo.get(k) / (double)(messageRecv.get(k) + messageLoss.get(k))));
                }
                //System.out.println(ooorate);

                //calculate the corresponding duplicate rate
                for (int k = 0; k < messageRecv.size(); k++) {
                    duperate.add(k, (messageDupe.get(k) / (double)(messageRecv.get(k) + messageLoss.get(k))));
                }
                //System.out.println(duperate);

                //topics which are waited for publishing
                String topicLanguage = "studentreport/u6251843/language";
                String topicTimestamp = "studentreport/u6251843/timestamp";
                String topicRecv = "studentreport/u6251843/" + (System.currentTimeMillis() / 1000L) + "/" + subqos+ "/recv";
                String topicLoss = "studentreport/u6251843/" + (System.currentTimeMillis() / 1000L) + "/" + subqos+ "/loss";
                String topicDupe = "studentreport/u6251843/" + (System.currentTimeMillis() / 1000L) + "/" + subqos+ "/dupe";
                String topicOoo = "studentreport/u6251843/" + (System.currentTimeMillis() / 1000L) + "/" + subqos+ "/ooo";

                MqttMessage message0 = new MqttMessage();
                //set payload for message0 (language)
                message0.setPayload("java".getBytes());
                //set QoS for message0
                message0.setQos(2);
                //set retained flag for the message0
                message0.setRetained(true);

                try {
                    // publish message to a topic on broker
                    client.publish(topicLanguage, message0);
                } catch (MqttException e) {
                    e.printStackTrace();
                }

                MqttMessage message1 = new MqttMessage();
                //set payload for message1 (timestamp)
                message1.setPayload(("" + (System.currentTimeMillis() / 1000L)).getBytes());
                message1.setQos(2);
                message1.setRetained(true);

                try {
                    client.publish(topicTimestamp, message1);
                } catch (MqttException e) {
                    e.printStackTrace();
                }


                MqttMessage message2 = new MqttMessage();
                //set payload for message2 (best received message in 1 min)
                message2.setPayload(("" + Collections.max(messageRecv)).getBytes());
                message2.setQos(2);
                message2.setRetained(true);

                try {
                    client.publish(topicRecv, message2);
                } catch (MqttException e) {
                    e.printStackTrace();
                }


                MqttMessage message3 = new MqttMessage();
                //set payload for message3 (worst message loss rate in 1 min)
                message3.setPayload(("" + Collections.max(lossrate)*100 + "%").getBytes());
                message3.setQos(2);
                message3.setRetained(true);

                try {
                    client.publish(topicLoss, message3);
                } catch (MqttException e) {
                    e.printStackTrace();
                }

                MqttMessage message4 = new MqttMessage();
                //set payload for message3 (worst message duplicate rate in 1 min)
                message4.setPayload(("" + Collections.max(duperate)*100 + "%").getBytes());
                message4.setQos(2);
                message4.setRetained(true);

                try {
                    client.publish(topicDupe, message4);
                } catch (MqttException e) {
                    e.printStackTrace();
                }


                MqttMessage message5 = new MqttMessage();
                //set payload for message3 (worst message out-of-order rate in 1 min)
                message5.setPayload(("" + Collections.max(ooorate)*100 + "%").getBytes());
                message5.setQos(2);
                message5.setRetained(true);

                try {
                    client.publish(topicOoo, message5);
                } catch (MqttException e) {
                    e.printStackTrace();
                }
                
            }
        }

        try {
            client.disconnect();
            System.exit(0);
        } catch (MqttException e) {
            e.printStackTrace();
        }

    }

    /**
     * The sumList function is designed to sum all the elements
     * before the index(exclusive)
     *
     * @param list An ArrayList which has more indexes than the
     *             index number (list.size() >= index)
     * @param index  An int determine the end index of the sum
     * @return the sum of (index - 1) numbers in the list
     */

    public int sumList(ArrayList<Integer> list, int index){
        Integer sum = 0;
        for (int i = 0; i < index; i++){
            sum += list.get(i);
        }
        return sum;
    }

    /**
     * The checkOrder function is designed to calculate the number
     * of out-of-order messages in the received messages
     *
     * @param list An ArrayList which stores the received messages
     * @param start  An int determines the start index of out-of-order check
     * @param end  An int determines the end index of out-of-order check
     * @return the numnber of messages which are out-of-order
     */

    public static int checkOrder(ArrayList<Integer> list, int start, int end){
        int counter = 0;
        if (start == 0){
            for (int i = 1; i <=end; i++){
                //check whether the number is in order
                if (!(list.get(i) - list.get(i-1) >= 0)){
                    counter++;
                }
            }
        }else {
            for (int i = start; i <=end; i++){
                if (!(list.get(i) - list.get(i-1) >= 0)){
                    counter++;
                }
            }
        }
        return counter;
    }

    /**
     * The isInt function is designed to judge whether the received messages are
     * Strings of integer or not
     *
     * @param s A string waited for being checked
     * @return True if the string is an 'integer' string
     */

    public boolean isInt(String s) {
        for(int a = 0; a < s.length(); a++){
            //check whether each character is a digit or not
            if( !Character.isDigit(s.charAt(a)) ) return false;
        }
        return true;
    }

    public List<Integer> mySort(List<Integer> input){
        List<Integer> outcome = new ArrayList<>(input.size());
        for (int i = 0; i < input.size(); i++){
            int min = i;
            for (int j = i + 1; j < input.size(); j++){
                if (input.get(j) < input.get(min)){
                    min = j;
                }
            }
            outcome.add(input.get(min));

            int temp = input.get(i);
            input.set(i,min);
            input.set(min,temp);
        }
        return outcome;
    }

    /**
     * The main function to run the program
     *
     */
    public static void main(String[] args) {
        MyMqttClient mqttClient = new MyMqttClient();
        mqttClient.run();
    }
}
