package com.huberlin.communication;

import com.huberlin.config.ForwardingTable;
import com.huberlin.event.Event;
import com.huberlin.communication.addresses.TCPAddressString;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

public class TCPEventSender implements SinkFunction<Tuple2<Integer, Event>> {
    static private final Logger log = LoggerFactory.getLogger(TCPEventSender.class);
    private final ForwardingTable fwd_table;
    private final Map<Integer, TCPAddressString> address_book;
    private final Map<TCPAddressString, PrintWriter> connections = new HashMap<>();
    private final  int nodeid;

    public TCPEventSender(Map<Integer, String> address_book,
                          ForwardingTable fwd_table,
                          int nodeid) {
            this.nodeid = nodeid;
            this.fwd_table = fwd_table;
            this.address_book = new HashMap<>();

            for (Integer node_id : fwd_table.get_all_node_ids())
                if (!address_book.containsKey(node_id))
                    throw new IllegalArgumentException("The address book does not have an entry for the node ID " + node_id);

            for (Integer node_id : address_book.keySet() )
                this.address_book.put(node_id, new TCPAddressString(address_book.get(node_id)));
    }

    @Override
    public void invoke(Tuple2<Integer, Event> tuple_of_source_node_id_and_event, Context ignored) {
        Integer source_node_id = tuple_of_source_node_id_and_event.f0;
        Event event = tuple_of_source_node_id_and_event.f1;
        for (Integer node_id : fwd_table.lookup(event.getEventType(), source_node_id)) {
            TCPAddressString dst = address_book.get(node_id);
            send_to(event.toString(), dst);
        }
    }

    private void send_to(String message, TCPAddressString target_ip_port) {
        try {
            if (!connections.containsKey(target_ip_port)) {
                try {
                    String host = target_ip_port.getHost();
                    int port = target_ip_port.getPort();

                    Socket client_socket = new Socket(host, port);
                    client_socket.setTcpNoDelay(true);
                    client_socket.setKeepAlive(true);
                    PrintWriter writer = new PrintWriter(client_socket.getOutputStream(), true);
                    writer.println("I am " + nodeid);
                    connections.put(target_ip_port, writer);
                    log.info("Connection for forwarding events to " + target_ip_port + " established");
                }
                catch (Exception e){
                    log.error("Failure to establish connection to " + target_ip_port + " for forwarding events. Error: " + e);
                    e.printStackTrace(System.err);
                    System.exit(1);
                }
            }
            connections.get(target_ip_port).println(message);
        } catch (Exception e) {
            log.warn("Forwarding Error: " + e + " - Message:" + message + " to " + target_ip_port);
            e.printStackTrace(System.err);
        }
    }

    @Override
    public void finish() {
        for (PrintWriter conn : connections.values()) {
            conn.flush();
        }
    }
}
