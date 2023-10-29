using Definitions;
using InterfaceLibraryConfigurator;
using InterfaceLibraryPublisher;
using Twingly.Gearman;
using System;
using System.Collections.Generic;
using System.Threading;
using InterfaceLibraryLogger;
using NLog;
using System.Text;

namespace PublisherGearman
{
    public class GearmanClient : IPublisher
    {
        private IConfigurator _configurator;
        private string _id;
        private Logger _logger;
        private Twingly.Gearman.GearmanClient _client = new Twingly.Gearman.GearmanClient();
        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _id = id;
                _logger = (Logger)logger.init("PublisherGearman");
                _logger.Trace("Inicio");

                _configurator = configurator;

                object value;
                Dictionary<string, object> dirpublisherConfig = _configurator.getMap(Config.Publishers, _id);

                if (!dirpublisherConfig.TryGetValue("host", out value ))
                {
                    _logger.Error($"No se encontro el parametro '{Config.Host}' en la configuracion del publicador con id '{_id}'");
                    throw new Exception($"No se encontro el parametro '{Config.Host}' en la configuracion del publicador con id '{_id}'");
                }
                string hostName = value.ToString();
                if (!dirpublisherConfig.TryGetValue("port", out value))
                {
                    _logger.Error($"No se encontro el parametro '{Config.Port}' en la configuracion del publicador con id '{_id}'");
                    throw new Exception($"No se encontro el parametro '{Config.Port}' en la configuracion del publicador con id '{_id}'");
                }
                string port = value.ToString(); 
                _logger.Debug("Cliente Gearman de '{0}:{1}'", hostName, port);
                _client.AddServer(hostName, Convert.ToInt32(port));

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
                if (payload is byte[])
                {
                    GearmanJobPriority gearmanPriority;
                    if (priority == "HIGH")
                    {
                        gearmanPriority = GearmanJobPriority.High;
                    }
                    else if (priority == "NORMAL")
                    {
                        gearmanPriority = GearmanJobPriority.Normal;
                    }
                    else if (priority == "LOW")
                    {
                        gearmanPriority = GearmanJobPriority.Low;
                    }
                    else   //Parametro de prioridad erroneo
                    {
                        _logger.Error("Parametro de prioridad erroneo '{0}'", priority);
                        _logger.Trace("Fin");
                        return false;
                    }
                    _client.SubmitBackgroundJob(recipient, (byte[])payload, gearmanPriority);

                    _logger.Debug("Recipient '{0}'  Payload '{1}'  Priority '{2}'", recipient, Encoding.UTF8.GetString((byte[])payload), priority);
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
