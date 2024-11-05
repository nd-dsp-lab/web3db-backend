using Microsoft.Extensions.Logging;
using Query.API.Kernel.Application.Query.Interfaces.Actions;
using Query.API.Kernel.Domain.Contracts.DTOs;
using Query.API.Kernel.Util.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Application.Query.Actions
{
    public class SendQueryAction : ClientApiProxy, ISendQueryAction
    {
        private ILogger<SendQueryAction> _logger;
        public SendQueryAction(ILogger<SendQueryAction> logger, HttpClient client) : base(logger, client)
        {
            _logger = logger;
        }
        public async Task<ReturnQueryResults> SendQueryAsync(SendQueryDTO query)
        {
            try
            {
                ReturnQueryResults response = await Retry.OnExceptionAsync(async () => await PostAsJsonAsync<ReturnQueryResults, SendQueryDTO>(query, "query/", ""), 3, 1000, nameof(SendQueryAsync));

                return response;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
