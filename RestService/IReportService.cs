using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace RestService
{
    public interface IReportService
    {
        string GetData(string endPoint);
        string CompletedPutData(string endPoint, string id);
    }
}
