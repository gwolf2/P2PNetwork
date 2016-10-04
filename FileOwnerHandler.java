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
 * Represents a thread of control utilized by a FileOwner object to connect with
 * peers to upload a file.
 */
class FileOwnerHandler extends Thread {

    //Maps chunk numbers (key) to the bytes that comprise the chunks (value)
    private Map<Integer, byte[]> chunks;

    //Output stream to send messages
    private ObjectOutputStream oout;

    //Input stream to receive messages
    private ObjectInputStream oin;

    //Name of file to be transferred
    private String fileName;

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
                case "PEER_INFO":
                    sendPeerInfo();
                    break;
                case "NEIGHBOR_PEER_INFO":
                    sendNeighborPeerInfo();
                    break;
                case "FILE_NAME":
                    sendFileName();
                    break;
                case "CHUNK_LIST":
                    sendChunkIdList();
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
     * Sends the peer's ID and port number.
     */
    private void sendPeerInfo() {

        //Get the assigned peerId and port number for the newly connected peer
        int peerId = this.assignPeerInfo();
        int portNumber = FileOwner.assignedPeerConfigInfo.get(peerId);

        //Send the peer ID and port number
        try {
            transfer(peerId);
            transfer(portNumber);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Sends a peer's neighbor information.
     */
    private void sendNeighborPeerInfo() {
        try {
            int peerId = oin.readInt(); //Read the peer ID

            //Send the config info for assigned peers
            transfer(FileOwner.assignedPeerConfigInfo);

            //Send the peer's download neighbor
            transfer((FileOwner.peerConfigInfo.get(peerId)[1]));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Sends the file name.
     */
    private void sendFileName() {
        try {
            transfer(this.fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Sends the chunk ID list.
     */
    private void sendChunkIdList() {

        //Extract the master chunk ID list from the chunks Map
        ArrayList<Integer> masterChunkIdList =
                new ArrayList<Integer>(this.chunks.size());
        for (int i = 0; i < this.chunks.size(); i++) {
            if (this.chunks.containsKey(i)) {
                masterChunkIdList.add(i);
            }
        }

        //Send the master chunk ID list
        try {
            transfer(masterChunkIdList);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Sends the requested chunk.
     */
    private void sendRequestedChunk() {
        try {
            int chunkNumber = this.oin.readInt(); //Read the requested chunk

            //Send the requested chunk
            transfer(chunkNumber);
            transfer(this.chunks.get(chunkNumber));

            int peerIdFromThreadName =
                    Integer.parseInt(this.getName().substring(7, 8)) % 5 + 1;
            System.out.println("Sent chunk " + chunkNumber + " to Peer " +
                    peerIdFromThreadName + ".");
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
    void storeChunks(Map<Integer, byte[]> chunks) {
        this.chunks = chunks;
    }

    /**
     * Sets the fileName variable.
     *
     * @param fileName The name of the file.
     */
    void fileName(String fileName) {
        this.fileName = fileName;
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
     * Finds the next available peer ID and port number and assigns them to the
     * new peer.
     *
     * @return The created peerID.
     */
    private int assignPeerInfo() {
        int peerId;

        //For each of up to 5 possible peer IDs...
        for (peerId = 1; peerId <= 5; peerId++) {

            //If the peer ID is not already assigned...
            if (!FileOwner.assignedPeerConfigInfo.containsKey(peerId)) {

                //Assign the peer ID and its associated port number
                FileOwner.assignedPeerConfigInfo.put(peerId,
                        (FileOwner.peerConfigInfo.get(peerId)[0]));
                break;
            }
        }
        return peerId;
    }

    /**
     * Transfers an object message to an output stream.
     *
     * @param message The object to transfer.
     */
    private void transfer(Object message) throws IOException {
        oout.writeObject(message);
        oout.flush();
        oout.reset();
    }

    /**
     * Transfers an int message to an output stream.
     *
     * @param message The int to transfer.
     */
    private void transfer(int message) throws IOException {
        oout.writeInt(message);
        oout.flush();
        oout.reset();
    }

    /**
     * Overrides the run method to create and start a new thread to allow peers
     * to connect and download file chunks.
     */
    @Override
    public void run() {
        processMessages();
    }
}