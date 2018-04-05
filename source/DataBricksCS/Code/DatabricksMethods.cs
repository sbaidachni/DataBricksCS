using DatabricksCSDemo.Code;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace DataBricksCS.Code
{
    class DatabricksMethods
    {
        private static string defaultClusterRegion = "https://eastus.azuredatabricks.net";

        public static async Task<ClusterList> GetClusterList(string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            var result = await client.GetStringAsync(
                $"{defaultClusterRegion}/api/2.0/clusters/list");

            var list = JsonConvert.DeserializeObject<ClusterList>(result);
            return list;
        }

        public static async Task StartCluster(string clusterId, string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            string contentString = $"{{\"cluster_id\":\"{clusterId}\"}}";
            HttpContent content = new StringContent(contentString);
            content.Headers.ContentType.MediaType = "application/json";
            var result=await client.PostAsync(
                $"{defaultClusterRegion}/api/2.0/clusters/start",content);
        }

        public static async Task<ClusterInfo> GetClusterInfo(string clusterId, string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            var result = await client.GetStringAsync(
                $"{defaultClusterRegion}/api/2.0/clusters/get?cluster_id={clusterId}");

            var cluster = JsonConvert.DeserializeObject<ClusterInfo>(result);
            return cluster;
        }

        public static async Task TerminateCluster(string clusterId, string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            string contentString = $"{{\"cluster_id\":\"{clusterId}\"}}";
            HttpContent content = new StringContent(contentString);
            content.Headers.ContentType.MediaType = "application/json";
            var result = await client.PostAsync(
                $"{defaultClusterRegion}/api/2.0/clusters/delete", content);
        }

        public static async Task<JobList> GetJobsList(string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            var result = await client.GetStringAsync(
                $"{defaultClusterRegion}/api/2.0/jobs/list");

            var list = JsonConvert.DeserializeObject<JobList>(result);
            return list;
        }

        public static async Task<int> CreateJob(JobSpSubmitCreateParameters job, string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            string contentString = JsonConvert.SerializeObject(job);
            HttpContent content = new StringContent(contentString);
            content.Headers.ContentType.MediaType = "application/json";
            var result = await client.PostAsync(
                $"{defaultClusterRegion}/api/2.0/jobs/create", content);
            var res = JsonConvert.DeserializeObject<JobCreateResult>(await result.Content.ReadAsStringAsync());
            return res.job_id;
        }

        public static async Task<SparkVersionList> GetSparkVersions(string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            var result = await client.GetStringAsync(
                $"{defaultClusterRegion}/api/2.0/clusters/spark-versions");

            var list = JsonConvert.DeserializeObject<SparkVersionList>(result);
            return list;
        }

        public static async Task<NodeTypeList> GetSparkNodes(string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            var result = await client.GetStringAsync(
                $"{defaultClusterRegion}/api/2.0/clusters/list-node-types");

            var list = JsonConvert.DeserializeObject<NodeTypeList>(result);
            return list;
        }

        public static async Task CreateCluster(ClusterCreateParameters par,string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            string contentString = JsonConvert.SerializeObject(par);
            HttpContent content = new StringContent(contentString);
            content.Headers.ContentType.MediaType = "application/json";
            var result = await client.PostAsync(
                $"{defaultClusterRegion}/api/2.0/clusters/create", content);
        }

        public static async Task<JobInfo> GetJobInfo(int jobId,string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            var result = await client.GetStringAsync(
                $"{defaultClusterRegion}/api/2.0/jobs/get?job_id={jobId}");

            var job = JsonConvert.DeserializeObject<JobInfo>(result);
            return job;
        }

        public static async Task<int> RunJobNow(int jobId, string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            string contentString = $"{{\"job_id\":{jobId}}}";
            HttpContent content = new StringContent(contentString);
            content.Headers.ContentType.MediaType = "application/json";
            var result = await client.PostAsync(
                $"{defaultClusterRegion}/api/2.0/jobs/run-now", content);
            var res = JsonConvert.DeserializeObject<JobRunResult>(await result.Content.ReadAsStringAsync());
            return res.run_id;
        }

        public static async Task<RunsInfo> GetRun(int runId, string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            var result = await client.GetStringAsync(
                $"{defaultClusterRegion}/api/2.0/jobs/runs/get?run_id={runId}");

            var run = JsonConvert.DeserializeObject<RunsInfo>(result);
            return run;
        }

        public static async Task<RunsList> GetRunList(int jobId, int offset,string tokenSecret)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenSecret);
            var result = await client.GetStringAsync(
                $"{defaultClusterRegion}/api/2.0/jobs/runs/list?job_id={jobId}&offset={offset}");

            var run = JsonConvert.DeserializeObject<RunsList>(result);
            return run;
        }

    }
}
