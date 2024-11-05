using Microsoft.Extensions.Logging;
using Query.API.Kernel.Application.Query.Interfaces;
using Query.API.Kernel.Application.Query.Interfaces.Actions;
using Query.API.Kernel.Domain.Contracts.DTOs;

namespace Query.API.Kernel.Application.Query
{
    public class QueryApplication : IQueryApplication
    {
        private ILogger<QueryApplication> _logger;
        private ISendQueryAction _sendQueryAction;
        
        public QueryApplication(ILogger<QueryApplication> logger, ISendQueryAction sendQueryAction)
        {
            _logger = logger;
            _sendQueryAction = sendQueryAction;
        }

        public async Task<ReturnQueryResults> SendQueryAndGetResultsAsync(SendQueryDTO queryDto)
        {
            try
            {
                ReturnQueryResults ret = await _sendQueryAction.SendQueryAsync(queryDto);
                return ret;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
