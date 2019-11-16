package de.hhu.cocri100.bigdata2019.project.source;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.io.FilenameUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static de.hhu.cocri100.bigdata2019.project.source.DataUtils.checkFileExists;
import static de.hhu.cocri100.bigdata2019.project.source.DataUtils.dataRoot;

public class DownloadKgsData {
    public static void main(String[] args) throws IOException, ZipException {

        // read data root folder
        ParameterTool params = ParameterTool.fromArgs(args);
        final String root = params.get("dataRoot", dataRoot);


        if (checkFileExists(root + "/KGS")) {
            System.out.println("KGS data was already found at " + root + "/KGS");
        } else {
            System.out.println("Downloading KGS data to " + root + "/KGS");
            downloadAndUnzipKgs(root);
        }
    }

    public static void downloadAndUnzipKgs(String root) throws IOException, ZipException {
        ArrayList<String> links = getDownloadLinks();
        ArrayList<String> paths = downloadKgs(root, links);
        unzipKgs(root, paths);
    }

    private static ArrayList<String> getDownloadLinks() throws IOException {
        // Connect to KGS Go Game DataSet website
        URL url = new URL("https://u-go.net/gamerecords/");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();

        // Get Html source code
        InputStream is = connection.getInputStream();
        InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(isr);

        // Filter for lines that contain zip download links
        String[] htmlLinks = br.lines().filter(f -> f.contains(".zip") && f.contains("href="))
                .map(String::trim).toArray(String[]::new);

        // Create regex to find download links
        String regex = "(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE|Pattern.DOTALL);

        ArrayList<String> links = new ArrayList<>();

        // Apply regex to the filtered lines and save download links
        for (String htmlLink : htmlLinks) {
            Matcher m = p.matcher(htmlLink);
            while (m.find()) {
                links.add(m.group());
            }
        }

        br.close();
        isr.close();
        is.close();
        connection.disconnect();

        return links;
    }

    private static ArrayList<String> downloadKgs(String root, ArrayList<String> links) throws IOException {
        // Create folder to save the zip files to
        String rootRawFolder = root + File.separator + "raw";
        File directory = new File(rootRawFolder);
        if (!directory.exists()){
            directory.mkdirs();
        }

        ArrayList<String> filePaths = new ArrayList<>();

        // Download each zip file and save the path to the downloaded filenames
        for (String link: links) {
            URL url = new URL(link);
            String name = FilenameUtils.getName(url.getPath());
            String filePath = rootRawFolder + File.separator + name;
            filePaths.add(filePath);

            File zipFile = new File(filePath);
            if (!zipFile.exists()){
                System.out.println(String.format("Downloading file %s", name));
                new FileOutputStream(filePath).getChannel()
                        .transferFrom(Channels.newChannel(url.openStream()), 0, Long.MAX_VALUE);
            } else {
                System.out.println(String.format("Skipped downloading %s because file already exists", name));
            }
        }

        return filePaths;
    }

    private static void unzipKgs(String root, ArrayList<String> filePaths) throws ZipException {
        // Create folder to save unzipped files to
        String rootDestinationFolder = root + File.separator + "KGS";

        // Unzip each downloaded file
        for (String filePath: filePaths) {
            String destinationFolder = rootDestinationFolder + File.separator + FilenameUtils.getBaseName(filePath);

            File directory = new File(destinationFolder);
            if (!directory.exists()){
                System.out.println(String.format("Unzipping %s to %s", FilenameUtils.getName(filePath), destinationFolder));
                ZipFile zipFile = new ZipFile(filePath);
                zipFile.extractAll(destinationFolder);
            } else {
                System.out.println(String.format("Skipped extracting %s because directory already exists.",
                        FilenameUtils.getName(filePath)));
            }
        }
    }
}
