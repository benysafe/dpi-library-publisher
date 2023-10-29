using Definitions;
using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryPublisher;
using System;
using System.Collections.Generic;
using System.Threading;
using NLog;
using System.Text;

namespace PublisherConsole
{
    public class ConsoleOut : IPublisher
    {
        private IConfigurator _configurator;
        private string _id;
        private Logger _logger;

        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _configurator = configurator;
                _id = id;
                _logger = (Logger)logger.init("PublisherConsole");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        public bool publish(string recipient, object payload, string priority)
        {
            try
            {
                _logger.Trace("Inicio");
                if (payload is byte[])
                {
                    string strPayload = Encoding.UTF8.GetString((byte[])payload);
                    _logger.Debug("Recipient '{0}'  Payload '{1}'", recipient, strPayload);
                    Console.WriteLine("Recipient:   {0} , Priority:  {1}", recipient, priority);
                    Console.WriteLine("Payload:   {0}", strPayload);
                    Console.WriteLine(" ");
                    _logger.Trace("Fin");
                    return true;
                }
                else
                {
                    _logger.Error("No está implementada la publicación para el tipo de dato suministrado");
                    return false;
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
    }
}
