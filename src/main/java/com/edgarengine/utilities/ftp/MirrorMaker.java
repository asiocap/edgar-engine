package com.edgarengine.utilities.ftp;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

/**
 * Created by jinchengchen on 4/26/16.
 */
public class MirrorMaker {
    private static Logger LOG = Logger.getLogger(MirrorMaker.class.getName());

    private String url = "ftp.sec.gov";
    private int port = 21;
    private String user_name = "anonymous";
    private String password = "loocalvinci@gmail.com";
    private String local_home;
    private final Semaphore semaphore = new Semaphore(0, true);

    public MirrorMaker(String url, String user_name, String password, String local_home) {
        this.url = url;
        this.user_name = user_name;
        this.password = password;
        this.local_home = local_home;

        new Timer().schedule(new TimerTask() {

            @Override
            public void run() {
                semaphore.release();
            }
        }, 0l, 4000l);
    }

    public boolean replicateFile(String file_path, boolean is_soft) {
        FTPClient ftpClient = new FTPClient();
        try {

            String local_file_path = local_home + file_path;
            if (is_soft && new File(local_file_path).exists()) {
                LOG.info(String.format("File %s exists, skipping download.", local_file_path));
                return true;
            }
            semaphore.acquire();

            ftpClient.connect(url, port);
            ftpClient.login(user_name, password);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            String parent_directory_path = local_file_path.substring(0, local_file_path.lastIndexOf(File.separator));
            File parent_directory = new File(parent_directory_path);
            if (!parent_directory.exists()) {
                parent_directory.mkdirs();
            }

            File replica_stream_tmp = new File(local_file_path + ".tmp");
            OutputStream replica_stream_stream = new BufferedOutputStream(new FileOutputStream(local_file_path + ".tmp"));
            boolean success = ftpClient.retrieveFile(file_path, replica_stream_stream);
            replica_stream_stream.close();

            if (success) {
                replica_stream_tmp.renameTo(new File(local_file_path));
                replica_stream_tmp.delete();
                System.out.printf("File %s has been downloaded successfully.\n", local_file_path);
            }

            return true;
        } catch (IOException ex) {
            System.out.println("Error: " + ex.getMessage());
            ex.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public boolean replicateDirectory(String directory_path, boolean is_soft, boolean deep_clone) {
        boolean successful = false;
        FTPClient ftpClient = new FTPClient();
        try {
            ftpClient.connect(url, port);
            ftpClient.login(user_name, password);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            FTPFile[] files = ftpClient.listFiles(directory_path);
            for (FTPFile file : files) {
                if (file.isFile()) {
                    System.out.println(file.getName());
                    if (!replicateFile(directory_path + File.separator + file.getName(), is_soft)) {
                        successful = false;
                    }
                }
            }

            if (deep_clone) {
                FTPFile[] sub_folders = ftpClient.listDirectories();
                if (sub_folders != null && sub_folders.length != 0) {
                    for (FTPFile folder : sub_folders) {
                        if (!replicateDirectory(directory_path + File.separator + folder.getName(), is_soft, true)) {
                            successful = false;
                        }
                    }
                }
            }
            return successful;
        } catch (IOException ex) {
            System.out.println("Error: " + ex.getMessage());
            ex.printStackTrace();
            return false;
        } finally {
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
