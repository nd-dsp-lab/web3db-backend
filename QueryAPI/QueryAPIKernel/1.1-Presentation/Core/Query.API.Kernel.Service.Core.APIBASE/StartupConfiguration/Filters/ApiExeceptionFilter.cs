using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Query.API.Kernel.Util.Exceptions;

namespace Query.API.Kernel.Service.Core.APIBASE.StartupConfiguration.Filters
{
    public class ApiExceptionFilter : ExceptionFilterAttribute
    {
        #region Attibutes
        private ILogger<ApiExceptionFilter> _logger;
        #endregion

        #region Constructors
        public ApiExceptionFilter() : base()
        {
        }
        public ApiExceptionFilter(ILogger<ApiExceptionFilter> logger) : base()
        {
            _logger = logger;
        }
        #endregion
        #region Methods
        public override void OnException(ExceptionContext context)
        {
            int statusCodeException = 500;
            string codeMessage = "INTERNAL_SERVER_ERROR";
            var errors = new List<Dictionary<string, string>>();

            if (context.Exception is AggregateException aggregateException)
            {
                statusCodeException = 404;  // Not found

                foreach (var exception in aggregateException.InnerExceptions)
                {
                    if (exception is PropertyNotFoundException propertyNotFoundException)
                    {
                        codeMessage = $"{propertyNotFoundException.PropertyName.ToUpper()}_NOT_FOUND";
                        var error = new Dictionary<string, string>
                {
                    {"Type", exception.GetType().ToString() },
                    {"Message", exception.Message },
                    {"Source", exception.Source },
                    {"StackTrace", exception.StackTrace },
                    {"CodeMessage", codeMessage }
                };
                        errors.Add(error);
                    }
                }
            }
            else
            {
                _logger?.LogError(context.Exception, $"Exception: {context.Exception.Message}");
                _logger?.LogError(context.Exception.InnerException, $"InnerExeception: {context.Exception?.InnerException?.Message}");

                var error = new Dictionary<string, string>
                {
                    {"Type", context.Exception.GetType().ToString() },
                    {"Message", context.Exception.Message },
                    {"Source", context.Exception.Source },
                    {"StackTrace", context.Exception.StackTrace },
                    {"CodeMessage", "INTERNAL_SERVER_ERROR" }
                };
                errors.Add(error);
            }

            context.Exception = context.Exception;
            context.HttpContext.Response.StatusCode = statusCodeException;
            context.Result = new JsonResult(errors);
            base.OnException(context);
        }


        #endregion
    }
}
