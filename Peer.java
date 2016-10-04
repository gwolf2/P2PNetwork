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
 * Represents a peer that can download from a file owner or other peers, and can
 * upload to other peers.
 */
public class Peer implements Runnable {

    //Maps chunk numbers (key) to the bytes that comprise the chunks (value)
    private static Map<Integer, byte[]> chunks = new HashMap<Integer, byte[]>();

    //Master list of all of the chunk IDs
    private static ArrayList<Integer> masterChunkIdList =
            new ArrayList<Integer>();

    //The file name
    private static String fileName;

    //The peer's ID
    private static int peerId;

    //The peer's port number
    private static int peerPortNumber;

    //The file owner's port number
    private int fileOwnerPortNumber;

    //The peer's download neighbor's port number
    private int downloadNeighborPortNumber = -1;

    //Output stream to send messages
    private ObjectOutputStream oout;

    //Input stream to receive messages
    private ObjectInputStream oin;

    /**
     * Constructs a Peer object and initializes its member variable,
     * fileOwnerPortNumber.
     *
     * @param fileOwnerPortNumber The file owner's port.
     */
    public Peer(int fileOwnerPortNumber) {
        this.fileOwnerPortNumber = fileOwnerPortNumber;
    }

    /**
     * Run the peer: set it up, make connections and transfer chunks.
     */
    private void runPeer() {
        try {

            //Create peer's socket to connect to file owner
            Socket peerSocket = new Socket("localhost", fileOwnerPortNumber);

            //Open input and output streams on the socket
            openStreams(peerSocket);

            //Get peer info, and connect to file owner, download neighbor, and
            // upload neighbor
            getPeerConfigInfo(oout, oin);
            getChunksFromFileOwner(oout, oin);
            connectToDownloadNeighbor();
            connectToUploadNeighbor();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get peer's config information and create folder for the peer's files.
     *
     * @param oout The output stream.
     * @param oin The input stream.
     */
    private void getPeerConfigInfo(ObjectOutputStream oout,
                                   ObjectInputStream oin) {
        try {

            //Request peer's ID and port number
            sendMessage(oout, "PEER_INFO");
            peerId = oin.readInt();
            peerPortNumber = oin.readInt();

            //Request and receive the download peer's ID and port number
            do {
                sendMessage(oout, "NEIGHBOR_PEER_INFO");
                sendMessage(oout, peerId);
                Map<Integer, Integer> neighborPeerInfo =
                        (Map<Integer, Integer>) oin.readObject();
                int downloadNeighborPid = (int) oin.readObject();

                if (neighborPeerInfo.containsKey(downloadNeighborPid)) {
                    downloadNeighborPortNumber =
                            neighborPeerInfo.get(downloadNeighborPid);
                } else {
                    downloadNeighborPortNumber = 0;
                }

                Thread.sleep(1000);
            } while (this.downloadNeighborPortNumber <= 0);

        } catch (IOException | InterruptedException |
                ClassNotFoundException e) {
            e.printStackTrace();
        }

        //Create a folder for the peer to save its chunks
        File peerFolder = new File("Peer" + peerId + "Files");
        if (!peerFolder.exists()) {
            peerFolder.mkdir();
        }
    }

    /**
     * Get initial chunks from the file owner.
     *
     * @param oout The output stream.
     * @param oin The input stream.
     */
    private void getChunksFromFileOwner(ObjectOutputStream oout,
                                        ObjectInputStream oin) {
        try {

            //Request and receive the file name
            sendMessage(oout, "FILE_NAME");
            fileName = (String) oin.readObject();

            //Request and receive the master chunk ID list from file owner
            sendMessage(oout, "CHUNK_LIST");
            masterChunkIdList = (ArrayList<Integer>) oin.readObject();
            System.out.println("Received chunk ID list from file owner.");

            //Request and receive initial chunks from file owner
            // e.g., Peer 1 will get chunk 0, chunk 5, chunk 10, and so on
            int requestedChunkNumber = (peerId - 1) % 5;
            while (requestedChunkNumber < masterChunkIdList.size()) {

                //Request chunk
                sendMessage(oout, "REQUEST_CHUNK");
                sendMessage(oout, requestedChunkNumber);
                System.out.println("Requested chunk " + requestedChunkNumber +
                        " from file owner.");

                //Receive and save chunk
                int receivedChunkNumber = oin.readInt();
                byte[] chunk = (byte[]) oin.readObject();
                chunks.put(receivedChunkNumber, chunk);
                saveChunk(receivedChunkNumber, chunk);
                System.out.println("Received chunk " + receivedChunkNumber +
                        " from file owner.");

                requestedChunkNumber += 5;
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Start connection with download neighbor.
     */
    private void connectToDownloadNeighbor() {

        //Now know download neighbor --> connect and start messaging
        (new Thread() {
            @Override
            public void run() {
                getChunksFromNeighbor();
            }
        }).start();
    }

    /**
     * Start connection with upload neighbor.
     */
    private void connectToUploadNeighbor() {

        //Create a server socket to allow upload neighbors to connect
        ServerSocket peerServerSocket = null;
        try {
            peerServerSocket = new ServerSocket(peerPortNumber);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {

            //Create PeerHandler thread for upload neighbor
            PeerHandler uploadNeighborSocket = new PeerHandler();

            try {

                //Accept connection to upload neighbor
                Socket socket = peerServerSocket.accept();

                //Open input and output streams on the socket
                uploadNeighborSocket.openStreams(socket);

                //Seed the handler with the peer's chunks
                uploadNeighborSocket.storeChunks(chunks);

                //Start the upload neighbor thread
                uploadNeighborSocket.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Connect with download neighbor and get chunks until all chunks are
     * received.
     */
    private void getChunksFromNeighbor() {
        try {
            Thread.sleep(15000);

            //Create peer's socket to connect to download neighbor
            Socket socketToReceive =
                    new Socket("localhost", downloadNeighborPortNumber);

            //Open input and output streams on the socket
            openStreams(socketToReceive);

            //Loop until peer has all chunks
            while (!hasAllChunks()) {

                System.out.println("Received chunk ID list from download " +
                        "neighbor.");

                //For all chunks...
                for (int chunkNumber = 0;
                     chunkNumber < masterChunkIdList.size(); chunkNumber++) {

                    //If the peer does not have the a particular chunk...
                    if (!chunks.containsKey(chunkNumber)) {

                        //Check if download neighbor has the chunk
                        sendMessage(oout, "CHECK_NEIGHBOR");
                        sendMessage(oout, chunkNumber);

                        //If download neighbor has the chunk...
                        if (oin.readInt() == 1) {

                            //Request the chunk
                            sendMessage(oout, "REQUEST_CHUNK");
                            sendMessage(oout, chunkNumber);
                            System.out.println("Requested chunk " + chunkNumber
                                    + " from download neighbor.");

                            //Receive and save the chunk
                            int position = oin.readInt();
                            byte[] chunk = (byte[]) oin.readObject();
                            chunks.put(position, chunk);
                            System.out.println("Received chunk " + chunkNumber
                                    + " from download neighbor.");
                        }
                    }
                }

                Thread.sleep(1000);
            }
        } catch (IOException | ClassNotFoundException |
                InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check if the peer has all the chunks. If the peer does not have all the
     * chunks, return false. If the peer does have all the chunks, combine the
     * chunks and return true.
     *
     * @return False if the peer does not have all the chunks, true if it does.
     */
    private boolean hasAllChunks() {

        //If there is a file on the chunk list that the peer does not have, then
        // the peer does not have all chunks: return false
        for (int chunk : masterChunkIdList) {
            if (!chunks.containsKey(chunk)) {
                return false;
            }
        }

        //Combine the chunks to create a copy of the original file
        combineChunks();

        //The peer has all the chunks: return true
        return true;
    }

    /**
     * Combines the chunks to create the peer's copy of the original file.
     */
    private void combineChunks() {
        try {

            //Create new file
            File file = new File("Peer" + peerId + "Files/" + fileName);
            if (file.exists()) {
                file.delete();
            }

            //Create a file output stream to the file
            FileOutputStream fos = new FileOutputStream(file);

            //Write each chunk to the file
            for (int i = 0; i < masterChunkIdList.size(); i++) {

                //Save the chunk
                saveChunk(i, chunks.get(masterChunkIdList.get(i)));

                //Write the chunk to the combined file
                fos.write(chunks.get(masterChunkIdList.get(i)));
            }
            fos.flush();
            fos.close();

            //Give the thread some time to finish its other operations
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            System.out.println("Download complete for Peer " + peerId + ".");
        } catch (FileNotFoundException e) {
            //Do nothing. This method will create the file if it is not found.
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Outputs a String value to an output stream.
     *
     * @param oout The output stream.
     * @param stringMessage The value to output.
     */
    private static void sendMessage(ObjectOutputStream oout,
                                    String stringMessage) throws IOException {
        oout.writeObject(stringMessage);
        oout.flush();
        oout.reset();
    }

    /**
     * Outputs an integer value to an output stream.
     *
     * @param oout The output stream.
     * @param intMessage The value to output.
     */
    private static void sendMessage(ObjectOutputStream oout,
                                    int intMessage) throws IOException {
        oout.writeInt(intMessage);
        oout.flush();
    }


    /**
     * Save chunk as a file.
     *
     * @param chunkNumber The chunk number.
     * @param chunk The chunk data.
     */
    private void saveChunk(int chunkNumber, byte[] chunk) {
        try {
            FileOutputStream fos = new FileOutputStream("Peer" + peerId +
                    "Files/" + chunkNumber, false);
            fos.write(chunk);
            fos.flush();
            fos.close();
        } catch (FileNotFoundException e) {
            //Do nothing. This method will create the file if it is not found.
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Reads the config.txt file to obtain a the file owner's port number.
     */
    private static int getFileOwnerPortNumber() throws FileNotFoundException {
        Scanner scanner = new Scanner(new FileInputStream("config.txt"));
        int fileOwnerPortNumber = scanner.nextInt();
        scanner.close();
        return fileOwnerPortNumber;
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
     * Overrides the run method to start a new Peer thread.
     */
    @Override
    public void run() {
        start();
    }

    /**
     * Starts Peer by calling the runPeer() method.
     */
    public void start() {
        System.out.println("Peer started.");
        runPeer();
    }

    /**
     * Instantiates and starts a Peer that connects to the file owner on the
     * file owner's port number, fileOwnerPortNumber.
     */
    public static void main(String[] args) throws FileNotFoundException {
        int fileOwnerPortNumber = getFileOwnerPortNumber();
        new Peer(fileOwnerPortNumber).start();
    }
}