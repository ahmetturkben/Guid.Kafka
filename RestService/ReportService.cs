using RestSharp;
using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace RestService
{
    public class ReportService : IReportService
    {
        private IHttpClientFactory _clientFactory;

        public ReportService(IHttpClientFactory httpClientFactory)
        {
            _clientFactory = httpClientFactory;
        }

        public string GetData(string endPoint)
        {
            var client = new RestClient("https://localhost:44379");
            var request = new RestRequest(endPoint, Method.GET);
            var queryResult = client.Execute(request);

            return queryResult?.Content;
        }

        public string CompletedPutData(string endPoint, string id)
        {
            var client = new RestClient("https://localhost:44379");
            var request = new RestRequest(endPoint + "/" + id, Method.PUT);
            
            var queryResult = client.Execute(request);

            return queryResult?.Content;
        }
    }
}
