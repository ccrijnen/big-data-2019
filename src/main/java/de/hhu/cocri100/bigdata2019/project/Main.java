package de.hhu.cocri100.bigdata2019.project;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static de.hhu.cocri100.bigdata2019.project.source.DataUtils.*;
import static de.hhu.cocri100.bigdata2019.project.source.DownloadKgsData.downloadAndUnzipKgs;

public class Main {
    private static final String format = "***************** %-20s *****************\n";

    public static void main(String[] args) throws Exception {

        Scanner input = new Scanner(System.in);

        System.out.format(format, StringUtils.center("Analyze Go Data",20));
        System.out.println();

        downloadCLI(input);
        runJobsCLI(input);
    }

    private static Boolean checkBatchResultsExist() {
        return checkFileExists(batchGameLengthStatsResultFileName) && checkFileExists(batchWinnerStatsResultFileName);
    }

    private static void runJobsCLI(Scanner input) throws Exception {

        HashMap<String, Integer> jobOrder = new HashMap<>();
        jobOrder.put("offline", 1);
        jobOrder.put("analyze", 2);
        jobOrder.put("compare", 3);
        jobOrder.put("predict", 4);

        boolean offline = false;
        TreeSet<String> onlineJobs = new TreeSet<>(Comparator.comparingInt(x -> jobOrder.getOrDefault(x, 0)));

        System.out.format(format, StringUtils.center("Available Jobs",20));
        System.out.println();

        System.out.println(String.format("* %-9s : Analyze Go data offline and save results", "\"offline\""));
        System.out.println(String.format("* %-9s   Compute (count/max/min/avg) Game Length and (black/white/none) Win Percentages", ""));
        System.out.println(String.format("* %-9s : Analyze Go data online and save results", "\"analyze\""));
        System.out.println(String.format("* %-9s   Compute hourly (count/max/min/avg) Game Length and (black/white/none) Win Percentages", ""));
        System.out.println(String.format("* %-9s : Analyze Go data online and compare with offline result", "\"compare\""));
        System.out.println(String.format("* %-9s   Compare hourly (count/max/min/avg) Game Length and (black/white/none) Win Percentages", ""));
        System.out.println(String.format("* %-9s   Predict hourly (avg) Game Length and (black/white/none) Win Percentages", "\"predict\""));
        System.out.println(String.format("* %-9s : Run all of the above consecutively", "\"all\""));

        while (true) {
            System.out.println("* Which Job or Jobs (separated by comma) do you want to run?");

            String line = input.nextLine();
            String[] jobs;

            if (line.equalsIgnoreCase("all")) {
                jobs = new String[]{"offline", "analyze", "compare", "predict"};
            } else {
                jobs = line.split(",");
                Arrays.sort(jobs, Comparator.comparingInt(x -> jobOrder.getOrDefault(x, 0)));
            }

            boolean shouldBreak = true;

            loop: for (String job: jobs) {
                switch (job.trim().toLowerCase()) {
                    case "offline":
                        offline = true;
                        break;
                    case "analyze":
                    case "compare":
                    case "predict":
                        onlineJobs.add(job);
                        break;
                    default:
                        shouldBreak = false;
                        offline = false;
                        onlineJobs = new TreeSet<>();
                        System.out.println("* Error: Invalid input \"" + job + "\"");
                        break loop;
                }
            }

            if (!checkBatchResultsExist() && onlineJobs.contains("compare") && !offline) {
                shouldBreak = false;
                offline = false;
                onlineJobs = new TreeSet<>();
                System.out.println("* Error: Scheduled Job \"compare\" while \"offline\" results don't exist");
            }

            if (shouldBreak) {
                break;
            }
        }

        StringBuilder jobString = new StringBuilder("* Scheduled jobs: ");
        if (offline) jobString.append("\"offline\"");
        for (String j: onlineJobs) {
            if (!offline) jobString.append("\"").append(j).append("\"");
            else jobString.append(", \"").append(j).append("\"");
        }
        System.out.println(jobString);

        Double fraction = readFraction(input);

        if (offline) {
            GoBatchJob goBatchJob = new GoBatchJob(fraction);
            System.out.println();
            System.out.format(format, StringUtils.center("Starting Job \"offline\"",20));
            System.out.println();
            goBatchJob.offlineAnalysis();
        }

        if (!onlineJobs.isEmpty()) {
            GoStreamingJob streamingJob = new GoStreamingJob(fraction);
            if (onlineJobs.contains("analyze")) {
                System.out.println();
                System.out.format(format, StringUtils.center("Starting Job \"analyze\"",20));
                System.out.println();
                streamingJob.onlineAnalysis();
            }
            if (onlineJobs.contains("compare")) {
                System.out.println();
                System.out.format(format, StringUtils.center("Starting Job \"compare\"",20));
                System.out.println();
                streamingJob.onlineComparison();
            }
            if (onlineJobs.contains("predict")) {
                System.out.println();
                System.out.format(format, StringUtils.center("Starting Job \"predict\"",20));
                System.out.println();
                streamingJob.onlinePrediction();
            }
        }
    }

    private static Double readFraction(Scanner input) {
        while (true) {
            System.out.println("* Please enter the fraction of the data you want to use [0 < fraction <= 1], default: 0.01");
            String line = input.nextLine().trim();
            double fraction;
            if (line.isEmpty()) {
                fraction = 0.01;
            } else {
                fraction = Double.parseDouble(line);
            }

            if (0 < fraction && fraction <= 1) {
                System.out.println(String.format("* Using %.1f%% of the data", fraction * 100));
                return fraction;
            }
        }
    }

    private static void downloadCLI(Scanner input) throws Exception {
        if (!checkFileExists(kgsRoot)) {
            System.out.format(format, StringUtils.center("KGS Data missing",20));

            scanner: while (true) {
                System.out.println("* Do you want to download it? [y/n]");

                String download = input.nextLine();

                switch (download.toLowerCase()) {
                    case "y":
                    case "yes":
                        System.out.println("* Downloading and unzipping KGS data to \"" + kgsRoot + "\"");
                        downloadAndUnzipKgs(dataRoot);
                        break scanner;
                    case "n":
                    case "no":
                        System.out.println("Not downloading KGS Data");
                        if (!checkFileExists(gogodRoot)) {
                            System.out.println("No Go Data found to analyze");
                            System.out.println("Exiting");
                            System.exit(0);
                        }
                        break scanner;
                    default:
                        System.out.println("Invalid input \"" + download + "\"");
                }
            }
        }
    }
}
