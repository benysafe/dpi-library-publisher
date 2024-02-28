using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryPublisher;
using Definitions;
using NLog;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Runtime.CompilerServices;

namespace PublisherTCP
{
    public class ClienteTCP : IPublisher
    {
        private IConfigurator _configurator;
        private string _id;
        private Logger _logger;
        private TcpClient _client =  new TcpClient();
        private string hostName;
        private string port;
        private int reconnectDelay = 5;
        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _id = id;
                _logger = (Logger)logger.init("PublisherTCP");
                _logger.Trace("Inicio");

                _configurator = configurator;

                getConfig();

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

                if (_configurator.hasNewConfig(_id))
                {
                    _client.Close();
                    getConfig();
                    _logger.Debug("Reconfiguracion exitosa");
                }

                byte[] bPayload = (byte[])payload;

                while(!_client.Connected)
                {
                    _logger.Error("Desconexion con el server TCP " + hostName + ":" + port+", se intentará reconectar");
                    _client.Connect(hostName, Convert.ToInt16(port));
                    Thread.Sleep(500);

                    if (_client.Connected)
                    {
                        _logger.Debug("Reconexion exitosa");
                        break;
                    }
                    Thread.Sleep(reconnectDelay * 1000);
                }

                //string[] socket = recipient.Split(':');
                if (payload is byte[])
                {
                    // Obtener el flujo de red para enviar y recibir datos
                    NetworkStream stream = _client.GetStream();

                    // Enviar los datos al servidor
                    stream.Write(bPayload, 0, bPayload.Length);

                    _logger.Debug("Recipient '" + recipient + "'\nPayload '" + Encoding.UTF8.GetString((byte[])payload) + "'\nPriority '" + priority + "'");
                    _logger.Trace("Fin");
                    return true;
                }
                else
                {
                    _logger.Error("No está implementada la publicación para el tipo de dato suministrado");
                    _logger.Trace("Fin");
                    return false;
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        private void getConfig()
        {
            try
            {
                _logger.Trace("Inicio");

                object value;
                Dictionary<string, object> dirpublisherConfig = _configurator.getMap(Config.Publishers, _id);

                if (!dirpublisherConfig.TryGetValue("host", out value))
                {
                    _logger.Error($"No se encontro el parametro '{Config.Host}' en la configuracion del publicador con id '{_id}'");
                    throw new Exception($"No se encontro el parametro '{Config.Host}' en la configuracion del publicador con id '{_id}'");
                }
                hostName = value.ToString();
                if (!dirpublisherConfig.TryGetValue("port", out value))
                {
                    _logger.Error($"No se encontro el parametro '{Config.Port}' en la configuracion del publicador con id '{_id}'");
                    throw new Exception($"No se encontro el parametro '{Config.Port}' en la configuracion del publicador con id '{_id}'");
                }
                port = value.ToString();
                _logger.Debug("Cliente TCP de " + hostName + ":" + port);


                if (!dirpublisherConfig.TryGetValue("reconnectDelay", out value))
                {
                    _logger.Error($"No se encontro el parametro 'reconnectDelay' en la configuracion del publicador con id '{_id}'");
                    throw new Exception($"No se encontro el parametro 'reconnectDelay' en la configuracion del publicador con id '{_id}'");
                }
                reconnectDelay = Convert.ToInt16(value.ToString());

                _client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

                _client.Connect(hostName, Convert.ToInt16(port));

                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
    }
}

