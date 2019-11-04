package KMeansDriver;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import KMeans.KMeans;
import Cluster.Cluster;
import KMeansCluster.KMeansCluster;
import RandomClusterGenerator.RandomClusterGenerator;

/**
 * 调度整个KMeans的运行过程
 * @author KING
 *
 */
public class KMeansDriver {
    private int k;
    private int iterationNum;
    private String sourcePath;
    private String outputPath;

    private Configuration conf;

    public KMeansDriver(int k, int iterationNum, String sourcePath, String outputPath, Configuration conf){
        this.k = k;
        this.iterationNum = iterationNum;
        this.sourcePath = sourcePath;
        this.outputPath = outputPath;
        this.conf = conf;
    }

    public void clusterCenterJob() throws IOException, InterruptedException, ClassNotFoundException{
        for(int i = 0;i < iterationNum; i++){
            Job clusterCenterJob = new Job();
            clusterCenterJob .setJobName("clusterCenterJob" + i);
            clusterCenterJob .setJarByClass(KMeans.class);

            clusterCenterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + i +"/");
            //上一句是上一次的输出作为这一次的输入，然后在mapper的setup里读取全局路径设置。第一次初始化initial的就算第一次了，i为0，在之前初始化就产生了，现在第一次迭代读取。
            clusterCenterJob.setMapperClass(KMeans.KMeansMapper.class);
            clusterCenterJob.setMapOutputKeyClass(IntWritable.class);
            clusterCenterJob.setMapOutputValueClass(Cluster.class);

            clusterCenterJob.setCombinerClass(KMeans.KMeansCombiner.class);
            clusterCenterJob.setReducerClass(KMeans.KMeansReducer .class);
            clusterCenterJob.setOutputKeyClass(NullWritable.class);
            clusterCenterJob.setOutputValueClass(Cluster.class);

            FileInputFormat.addInputPath(clusterCenterJob, new Path(sourcePath));
            FileOutputFormat.setOutputPath(clusterCenterJob, new Path(outputPath + "/cluster-" + (i + 1) +"/"));

            clusterCenterJob.waitForCompletion(true);
            System.out.println("finished!");
        }
    }           //每次迭代，先setup读取上一次的中心点文件，然后map读取instance.txt文件，进行分类，然后combine，然后reduce，计算新的中心点，然后输出。

    public void KMeansClusterJod() throws IOException, InterruptedException, ClassNotFoundException{
        Job kMeansClusterJob = new Job();
        kMeansClusterJob.setJobName("KMeansClusterJob");
        kMeansClusterJob.setJarByClass(KMeansCluster.class);

        kMeansClusterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + (iterationNum - 1) +"/");

        kMeansClusterJob.setMapperClass(KMeansCluster.KMeansClusterMapper.class);
        kMeansClusterJob.setMapOutputKeyClass(Text.class);
        kMeansClusterJob.setMapOutputValueClass(IntWritable.class);

        kMeansClusterJob.setNumReduceTasks(0);

        FileInputFormat.addInputPath(kMeansClusterJob, new Path(sourcePath));
        FileOutputFormat.setOutputPath(kMeansClusterJob, new Path(outputPath + "/clusteredInstances" + "/"));

        kMeansClusterJob.waitForCompletion(true);
        System.out.println("finished!");
    }      //最后再读取一边上次也就是最后一次迭代后的中心点，然后对每一个instance分类，输出。

    public void generateInitialCluster(){
        RandomClusterGenerator generator = new RandomClusterGenerator(conf, sourcePath, k);
        generator.generateInitialCluster(outputPath + "/");
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        System.out.println("start");
        Configuration conf = new Configuration();
        int k = Integer.parseInt(args[0]);
        int iterationNum = Integer.parseInt(args[1]);
        String sourcePath = args[2];
        String outputPath = args[3];
        KMeansDriver driver = new KMeansDriver(k, iterationNum, sourcePath, outputPath, conf);
        driver.generateInitialCluster();
        System.out.println("initial cluster finished");
        driver.clusterCenterJob();
        driver.KMeansClusterJod();
    }
}

