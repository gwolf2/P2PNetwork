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
 * Represents a file owner that uploads a file to a group of peers.
 */
public class FileOwner {

    //Files to be broken into chunks of 100KB
    private static final int CHUNK_SIZE = 102400;

    //Name of file to be shared (as input by user)
    private static String fileName;

    //Maps chunk numbers (key) to the bytes that comprise the chunks (value)
    private static Map<Integer, byte[]> chunks = new HashMap<Integer, byte[]>();

    //Maps peers (key) to their port numbers (value).
    // For peers who have connected and been assigned IDs
    static Map<Integer, Integer> assignedPeerConfigInfo =
            new HashMap<Integer, Integer>();

    //Maps peers (key) to their ports and neighbors (value).
    // Note: In the array, index 0 is the peer's port number, index 1 is the
    // peer's download neighbor, and index 2 is the peer's upload neighbor
    static Map<Integer, Integer[]> peerConfigInfo =
            new HashMap<Integer, Integer[]>();

    //FileOwner socket. Waits for requests from peers in the network, performs
    // an operation based on the type of request
    private ServerSocket fileOwnerSocket;

    //The FileOwner's port number
    private static int portNumber;

    /**
     * Constructs a FileOwner object by:
     * (1) initializing its port number
     * (2) creating a new ServerSocket, bound to the port
     * (3) calling splitFile() to break the file into chunks
     *
     * @param portNumber The port.
     */
    public FileOwner(int portNumber) {

        //Create socket for peers to connect to
        try {
            this.fileOwnerSocket = new ServerSocket(portNumber);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Break the file into chunks
        this.splitFile();
    }

    /**
     * Scans the config.txt file to initialize the FileOwner and peer IDs and
     * ports.
     */
    private static void loadConfig() {
        try {
            Scanner scanner = new Scanner(new FileInputStream("config.txt"));

            portNumber = scanner.nextInt(); //Read file owner's port number
            while (scanner.hasNext()) {
                int peerId = scanner.nextInt(); //Read peer ID
                Integer[] peerInfo = new Integer[3];
                peerInfo[0] = scanner.nextInt(); //Read peer's port number
                peerInfo[1] = scanner.nextInt(); //Read peer's download neighbor
                peerInfo[2] = scanner.nextInt(); //Read peer's upload neighbor
                peerConfigInfo.put(peerId, peerInfo); //Store info
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Splits a file into 100KB chunks and store them in the Chunks folder.
     */
    private void splitFile() {
        try {

            //Create folder to store the chunks
            File fileOwnerFolder = new File("Chunks");
            if (!fileOwnerFolder.exists()) {
                fileOwnerFolder.mkdir();
            }

            //Create input stream to the file
            FileInputStream fis = new FileInputStream(fileName);

            //Byte array of size CHUNK_SIZE
            byte[] maxChunkSize = new byte[CHUNK_SIZE];
            int sizeOfCurrentChunk; //Size of chunk currently being read
            int chunkNumber = 0; //Initialize first chunk number

            //Read the entire file into chunks of CHUNK_SIZE*
            while ((sizeOfCurrentChunk = fis.read(maxChunkSize)) != -1) //
            {
                //Copy a CHUNK_SIZE* range of bytes from maxChunkSize
                byte[] chunkInBytes =
                        Arrays.copyOfRange(maxChunkSize, 0, sizeOfCurrentChunk);

                //Store the chunk in the chunks Map
                chunks.put(chunkNumber, chunkInBytes);

                //Write the chunk to the "Chunks" folder
                FileOutputStream fos =
                        new FileOutputStream("Chunks/" + chunkNumber, false);
                fos.write(chunkInBytes);
                fos.flush();
                fos.close();

                chunkNumber++; //Move on to next chunk
            }
            //*Except the last chunk, which may be smaller than CHUNK_SIZE
        } catch (IOException e) {
            System.out.println("Error: Invalid file name. Make sure the file " +
                    "is located in this folder.");
            System.exit(1);
        }
    }

    /**
     * Overrides the start method to create and start a new thread to allow
     * peers to connect and download file chunks.
     */
//    @Override
    public void start() {
        try {
            while (true) {

                //Accept connection
                Socket socket = fileOwnerSocket.accept();

                //Create new file owner handler thread for the connection
                FileOwnerHandler fileOwnerHandler = new FileOwnerHandler();

                //Seed the file owner handler with the chunks and file name
                fileOwnerHandler.storeChunks(chunks);
                fileOwnerHandler.fileName(fileName);

                //Open input and output streams and start the thread
                fileOwnerHandler.openStreams(socket);
                fileOwnerHandler.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Instantiates and starts a new FileOwner object.
     */
    public static void main(String[] args) throws IOException {

        //Load peer IDs and ports from config.txt file
        loadConfig();

        System.out.println("File owner started.");

        //Prompt user for file name
        BufferedReader br =
                new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter full file name:");
        fileName = br.readLine();

        System.out.println("File owner is now accepting connections from " +
                "Peers.");

        //Create and start a FileOwner object
        new FileOwner(portNumber).start();
    }
}