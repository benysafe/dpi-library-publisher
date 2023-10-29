using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryPublisher;
using Definitions;
using NLog;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace PublisherUDP
{
    public class UDP : IPublisher
    {
        private IConfigurator _configurator;
        private string _id;
        private Logger _logger;
        private UdpClient _client;

        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _id = id;
                _logger = (Logger)logger.init("PublisherUDP");
                _logger.Trace("Inicio");

                _configurator = configurator;

                _client = new UdpClient();
                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        public bool publish(string recipient, object payload, string priority = "NORMAL")
        {
            try
            {
                _logger.Trace("Inicio");
                byte[] bPayload = (byte[])payload;
                string[] socket = recipient.Split(':');                
                if (payload is byte[])
                {
                    _client.Send(bPayload, bPayload.Length, socket[0], Convert.ToInt16(socket[1]));
                    _logger.Debug("Recipient '{0}:{1}'  Payload '{2}' ", socket[0],socket[1], Encoding.UTF8.GetString((byte[])payload));
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

