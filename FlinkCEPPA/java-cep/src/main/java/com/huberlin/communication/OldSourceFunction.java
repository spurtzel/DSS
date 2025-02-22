package com.huberlin.communication;

import com.huberlin.event.Event;
import com.huberlin.event.SimpleEvent;  
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList; 

public class OldSourceFunction extends RichSourceFunction<Tuple2<Integer,Event>> {
    private static final Logger log = LoggerFactory.getLogger(OldSourceFunction.class);
    private volatile boolean isCancelled = false;
    private BlockingQueue<Tuple2<Integer, Event>> merged_event_stream;
    private final int port;

    public OldSourceFunction(int listen_port){
        super();
        this.port = listen_port;
    }

    @Override
    public void run(SourceFunction.SourceContext<Tuple2<Integer,Event>> sourceContext) throws Exception {
        long timestamp_us = Long.MIN_VALUE; 
        while (!isCancelled) {
            Tuple2<Integer, Event> event_with_source_node_id = merged_event_stream.take();
            timestamp_us = Math.max(LocalTime.now().toNanoOfDay() / 1000L, timestamp_us+1);
            sourceContext.collectWithTimestamp(event_with_source_node_id, timestamp_us);
            sourceContext.emitWatermark(new Watermark(timestamp_us));
            if (log.isTraceEnabled()) {
                log.trace("From node " + event_with_source_node_id.f0 + ": "+
                        "Event " + event_with_source_node_id.f1 + " collected with timestamp " + timestamp_us);
                log.trace("Watermark: " + timestamp_us);
            }
        }
    }
    @Override
    public void cancel() {
        isCancelled = true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        merged_event_stream = new LinkedBlockingQueue<>();

        Thread new_connection_accepting_thread = new Thread(() -> {
            try (ServerSocket accepting_socket = new ServerSocket()) {
                accepting_socket.bind(new InetSocketAddress(port));
                log.debug("Bound server socket " + accepting_socket);
                while (!isCancelled) {
                    try {
                        Socket new_client = accepting_socket.accept();
                        new_client.shutdownOutput(); 
                        new Thread(() -> handleClient(new_client)).start();
                        log.info("New client connected: " + new_client);
                    }
                    catch (IOException e) {
                        log.warn("Exception in connection-accepting thread: " +  e);
                    }
                }
                log.debug("Not accepting additional connections.");
            } catch (IOException e) {
                log.error("Failed to open server socket: " + e);
                e.printStackTrace(System.err);
                System.exit(1);
            }
        });
        new_connection_accepting_thread.start();
    }
    public void handleClient(Socket socket) {
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String client_address = socket.getRemoteSocketAddress().toString();
            Integer client_node_id = null;
            while (!isCancelled) {
                String message = input.readLine();
                if (message == null || message.contains("end-of-the-stream")) {
                    log.info("Reached the end of the stream for " + client_address);
                    if (message == null)
                        log.warn("Stream terminated without end-of-the-stream marker.");
                   
                    merged_event_stream.put(new Tuple2<>(client_node_id, 
                        new SimpleEvent("SENTINEL-" + client_node_id,
                                        LocalTime.now().toNanoOfDay() / 1000L,
                                        "end-of-stream",
                                        0,
                                        new ArrayList<>())));
                    input.close();
                    socket.close();
                    return;
                }
                else if (client_node_id == null) {
                    if (! message.startsWith("I am ")) {
                        log.error("The first line from a newly connected client must be 'I am '"
                                + " followed by the client's node_id.\n"
                                + " Received: " + message + "\n from remote socket: " + client_address);
                        input.close();
                        socket.close();
                        return;
                    }
                    client_node_id = Integer.valueOf(message.substring(5));
                    log.info("Client " + client_address + " introduced itself as node " + client_node_id);
                }
                else if (message.contains("|")){
                    Event event = Event.parse(message);
                    merged_event_stream.put(new Tuple2<>(client_node_id, event));
                    LocalTime currTime = LocalTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss:SSSSSS");
                    System.out.println(event + " was received at: " + currTime.format(formatter));
                }
                else {
                    log.debug("Ignoring message:" + message);
                }
            }
        } catch (IOException | InterruptedException e) {
            log.info("Client disconnected");
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }
}
