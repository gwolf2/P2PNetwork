/*******************************************************************************
 * Name: Gavin Wolf
 * UFID: 75172370
 * Course: Computer Networks - CNT5106C
 * Professor: Dr. Shigang Chen
 * Due Date: 7/25/2016
 * Assignment: Project - P2P Network
 * Description: A peer-to-peer network for file sharing.
 ******************************************************************************/

import java.lang.*;
import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Represents a thread of control utilized by a Peer object to connect with
 * other peers to transfer files.
 */
class PeerHandler extends Thread {

    //Maps chunk numbers (key) to the bytes that comprise the chunks (value)
    private Map<Integer, byte[]> chunks;

    //Output stream to send messages
    private ObjectOutputStream oout;

    //Input stream to receive messages
    private ObjectInputStream oin;

    /**
     * Receives message and sends an appropriate response.
     */
    private void processMessages() {

        //Wait for messages, then respond
        while (true) {

            //Read the message and make sure it is formatted as a String
            Object quantity;
            while (true) {
                try {
                    quantity = this.oin.readObject();
                    assert (quantity instanceof String);
                    break;
                } catch (Exception ignored) {
                }
            }
            String message = (String) quantity;

            //Process message depending on its type
            switch (message) {
                case "CHECK_NEIGHBOR":
                    sendCheckNeighbor();
                    break;
                case "REQUEST_CHUNK":
                    sendRequestedChunk();
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Receives requested chunk number. Sends 1 if it has the chunk, sends 0
     * otherwise.
     */
    private void sendCheckNeighbor() {
        try {
            int chunkNumber = this.oin.readInt(); //Read the chunk number

            //Respond with 1 if it has the chunk; 0 otherwise
            if (this.chunks.containsKey(chunkNumber)) {
                transfer(1);
            } else {
                transfer(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Sends requested chunk.
     */
    private void sendRequestedChunk() {
        try {
            int chunkNumber = this.oin.readInt(); //Read the requested chunk
            System.out.println("Upload neighbor requested chunk " +
                    chunkNumber + ".");

            //Send the requested chunk
            transfer(chunkNumber);
            transfer(this.chunks.get(chunkNumber));
            System.out.println("Sent chunk " + chunkNumber + " to upload " +
                    "neighbor.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Stores chunk information in a Map variable.
     *
     * @param chunks A Map that maps chunk numbers (key) to the bytes that
     *               comprise the chunk (value).
     */
    public void storeChunks(Map<Integer, byte[]> chunks) {
        this.chunks = chunks;
    }

    /**
     * Creates an output stream and an input stream for a given socket.
     *
     * @param socket The socket.
     */
    void openStreams(Socket socket) throws IOException {
        oout = new ObjectOutputStream(socket.getOutputStream());
        oin = new ObjectInputStream(socket.getInputStream());
    }

    /**
     * Transfers an object message to an output stream.
     *
     * @param objectMessage The object to transfer.
     */
    private void transfer(Object objectMessage) throws IOException {
        oout.writeObject(objectMessage);
        oout.flush();
        oout.reset();
    }

    /**
     * Transfers an int message to an output stream.
     *
     * @param intMessage The int to transfer.
     */
    private void transfer(int intMessage) throws IOException {
        oout.writeInt(intMessage);
        oout.flush();
        oout.reset();
    }

    /**
     * Overrides the run method to start a new thread to allow peers to connect.
     */
    @Override
    public void run() {
        processMessages();
    }
}