using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Query.API.Kernel.Service.Core.APIBASE.StartupConfiguration.Filters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Service.Core.APIBASE.StartupConfiguration.Base
{
    [ApiController]
    [Route("api/[controller]")]
    [ServiceFilter(typeof(ApiExceptionFilter))]

    public abstract class ApiControllerBase<T> : ControllerBase
    {
        #region Attributes
        protected ILogger<T> _logger;
        #endregion
        #region Constructor
        public ApiControllerBase(ILogger<T> logger)
        {
            _logger = logger;
        }
        #endregion
    }
}
