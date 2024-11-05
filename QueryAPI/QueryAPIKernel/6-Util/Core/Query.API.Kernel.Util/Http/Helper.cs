using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Util.Http
{
    #region Message Extensions
    public static class HttpResponseMessageExtensions
    {
        #region Methods
        public static async Task EnsureSuccessStatusCodeAsync(this HttpResponseMessage response)
        {
            if (response.IsSuccessStatusCode)
            {
                return;
            }

            var content = await response.Content.ReadAsStringAsync();

            if (response.Content != null)
                response.Content.Dispose();

            throw new SimpleHttpResponseException(response.StatusCode, content);
        }
        #endregion
    }
    #endregion
    #region Response Exception
    public class SimpleHttpResponseException : Exception
    {
        #region Attributes
        public HttpStatusCode StatusCode { get; private set; }
        #endregion
        #region Constructor
        public SimpleHttpResponseException(HttpStatusCode statusCode, string content) : base(content)
        {
            StatusCode = statusCode;
        }
        #endregion
    }
    #endregion
    #region Retry
    public class Retry
    {
        #region Attributes
        private static ILogger<Retry> _logger;//TODO = SerilogManager.CreateLogger<Retry>();
        #endregion
        #region Methods
        public static async Task<TRet> OnExceptionAsync<TRet>(Func<Task<TRet>> callBackDelegate, int numberOfTries = 3, int delayBetweenTries = 500, string caller = "")
        {
            Exception lastException = null;
            for (var currentTry = 1; currentTry <= numberOfTries; currentTry++)
            {
                try
                {
                    return await callBackDelegate();
                }
                catch (HttpRequestException ex)
                {
                    _logger.LogError(ex, ex.Message);
                    _logger.LogInformation($"{caller}[ReturnType: {nameof(TRet)}]\n {ex.Message}\n{ex.StackTrace}\n{ex.ToString()}");
                    if (ex.InnerException != null)
                    {
                        lastException = ex;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                    _logger.LogInformation($"{caller} - {ex.Message}\n{ex.StackTrace}\n{ex.ToString()}\nInnerMessage:{ex.InnerException?.Message}\nInner Stack:{ex.InnerException?.StackTrace}");
                }
                Thread.Sleep(delayBetweenTries);
            }
            if (lastException != null)
            {
                throw lastException;
            }
            throw new Exception("No exception to rethrow");
        }
        public static TRet OnException<TRet>(Func<TRet> callBackDelegate, int numberOfTries = 3, int delayBetweenTries = 500, string caller = "")
        {
            Exception lastException = null;
            for (var currentTry = 1; currentTry <= numberOfTries; currentTry++)
            {
                try
                {
                    return callBackDelegate();
                }
                catch (HttpRequestException ex)
                {
                    _logger.LogError(ex, ex.Message);
                    _logger.LogInformation($"{caller}[ReturnType: {nameof(TRet)}]\n {ex.Message}\n{ex.StackTrace}\n{ex.ToString()}");
                    if (ex.InnerException != null)
                    {
                        _logger.LogInformation($"[InnerException]{caller} Target: {ex.InnerException.TargetSite}\n {ex.InnerException.Message}\n{ex.InnerException.StackTrace}\n{ex.InnerException.ToString()}");
                        lastException = ex;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                    _logger.LogInformation($"{caller} - {ex.Message}\n{ex.StackTrace}\n{ex.ToString()}" +
                        $"InnerException:{ex.InnerException?.Message}\nInnerExceptionStack:{ex.InnerException?.StackTrace}");
                }
                Thread.Sleep(delayBetweenTries);
            }
            if (lastException != null)
            {
                throw lastException;
            }
            throw new Exception("No exception to rethrow");
        }
        #endregion
    }
    #endregion
}
