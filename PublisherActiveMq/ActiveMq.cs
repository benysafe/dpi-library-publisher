using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryPublisher;
using Definitions;
using NLog;
using Apache.NMS;
using Apache.NMS.Util;
using System.Text;

namespace PublisherActiveMq
{
    public class ActiveMq : IPublisher
    {
        private IConfigurator _configurator;
        private string _id;
        private Logger _logger;

        //private Uri connecturi;
        //private IConnectionFactory factory;

        private IConnection connection;
        private ISession session;
        private IDestination destination;
        private bool _toQueue;

        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _configurator = configurator;
                _id = id;
                _logger = (Logger)logger.init("PublisherActiveMq");

                object strUri;
                object strTypeOut;
                Dictionary<string, object> dirpublisherConfig = _configurator.getMap(Config.Publishers, _id);
                if(!dirpublisherConfig.TryGetValue(Config.Uri, out strUri))
                {
                    _logger.Error($"No se encontro el parametro '{Config.Uri}' en la configuracion del publicador con id '{_id}'");
                    throw new Exception($"No se encontro el parametro '{Config.Uri}' en la configuracion del publicador con id '{_id}'");
                }
                if (!dirpublisherConfig.TryGetValue(Config.ActiveMQTypeOut, out strTypeOut))
                {
                    _logger.Error($"No se encontro el parametro '{Config.ActiveMQTypeOut}' en la configuracion del publicador con id '{_id}'");
                    throw new Exception($"No se encontro el parametro '{Config.ActiveMQTypeOut}' en la configuracion del publicador con id '{_id}'");
                }
                Uri connecturi = new Uri(strUri.ToString());
                if(strTypeOut.ToString() == "queue")
                {
                    _toQueue = true;
                }
                else if(strTypeOut.ToString() == "topic")
                {
                    _toQueue = false;
                }
                

                IConnectionFactory factory = new Apache.NMS.ActiveMQ.ConnectionFactory(connecturi);

                connection = factory.CreateConnection();
                
                connection.Start();

                session = connection.CreateSession();

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
                if (payload is byte[])
                {
                    if (_toQueue)
                        destination = session.GetQueue(recipient);
                    else
                        destination = session.GetTopic(recipient);

                    IMessageProducer producer = session.CreateProducer(destination);

                    producer.DeliveryMode = MsgDeliveryMode.Persistent;
                    producer.DisableMessageTimestamp = true;
                    //producer.RequestTimeout = receiveTimeout;     //ToDo: ???
                    if (priority == "LOW")
                        producer.Priority = MsgPriority.Low;
                    else if (priority == "HIGH")
                        producer.Priority = MsgPriority.High;
                    else
                        producer.Priority = MsgPriority.Normal;

                    ITextMessage request = session.CreateTextMessage(Encoding.UTF8.GetString((byte[])payload));
                    //request.NMSCorrelationID = "abc";                 //ToDo: ???
                    //request.Properties["NMSXGroupID"] = "cheese";     //ToDo: ???
                    //request.Properties["myHeader"] = "Cheddar";       //ToDo: ???

                    producer.Send(request);
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