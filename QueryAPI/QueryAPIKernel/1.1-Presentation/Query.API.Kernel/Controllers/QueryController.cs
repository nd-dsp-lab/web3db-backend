using Microsoft.AspNetCore.Mvc;
using Query.API.Kernel.Application.Query.Interfaces;
using Query.API.Kernel.Domain.Contracts.DTOs;
using Query.API.Kernel.Service.Core.APIBASE.StartupConfiguration.Base;
using Serilog.Context;

namespace Query.API.Kernel.Controllers
{
    public class QueryController : ApiControllerBase<QueryController>
    {
        #region Attributes
        IQueryApplication _queryApplication;
        #endregion
        public QueryController(ILogger<QueryController> logger, IQueryApplication queryApplication) : base(logger)
        {
            _logger = logger;
            _queryApplication = queryApplication;
        }
        [HttpPost("SendQuery")]
        public async Task<IActionResult> PostNewQuery([FromBody] SendQueryDTO sendQueryDTO)
        {
            try
            {
                using (LogContext.PushProperty("Fetching all schemas ", ""))
                {
                    _logger.LogInformation("Fetch all schemas in a database");

                    ReturnQueryResults response = await _queryApplication.SendQueryAndGetResultsAsync(sendQueryDTO);
                    if (response != null)
                    {
                        return Ok(response);
                    }
                    else
                    {
                        return NoContent();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error on FetchAllSchemas");
                throw ex;
            }
        }
    }
}
