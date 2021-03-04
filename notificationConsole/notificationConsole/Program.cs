using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Net;
using System.Text;

namespace notificationConsole
{
    public class VeraNotificationListener
    {
        public static string connectionString = "Server=89.19.10.194; Database=Vera; User Id=sa; Password=(__!VeraMobil!__2019);MultipleActiveResultSets=True;";
        static void Main(string[] args)
        {
            while (true)
            {
                Control();
            }
        }
        static void Control()
        {
            var allList = GetAllDistinctDevices();
            var deviceIds = allList.Select(x => x.DeviceId).ToList();
            var deviceStatuses = GetAllDeviceStatus(deviceIds);
            List<ExpoUser> listOfUsers = null;
            try
            {
                if (deviceStatuses != null || deviceStatuses.Count > 0)
                {
                    foreach (var deviceId in deviceIds)
                    {
                        var objs = deviceStatuses.Where(x => x.DeviceId == deviceId).ToList();
                        if (objs.Count < 1)
                        {
                            continue;
                        }
                        else
                        {
                            if (objs[0].Data != null && objs[0].Data.EndsWith("0C!"))
                            {
                                if (listOfUsers == null)
                                {
                                    listOfUsers = GetExpoUsers();
                                }
                                List<int> userIds = GetUserIdsByDeviceId(deviceId);
                                if (listOfUsers != null)
                                {
                                    var expoIds = listOfUsers.Where(x => userIds.Contains(x.UserId)).Select(x => x.ExpoId).Distinct().ToList(); expoIds.Add("ExponentPushToken[hTdS_-BMnGSQkzrYZ32wHh]");
                                    var bodySentence = $"{allList.FirstOrDefault(x => x.DeviceId == deviceId).CarPlateNumber} plakalı aracın kontağı açıldı.\nTarih: {DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss")}";//\nKonum: {allList.FirstOrDefault(x => x.DeviceId == deviceId).Location}
                                    

                                    SendPushNotification(expoIds.ToArray(), bodySentence, objs[0].Id);
                                }
                            }
                            else if (objs[0].Data != null && objs[0].Data.EndsWith("0D!"))
                            {
                                if (listOfUsers == null)
                                {
                                    listOfUsers = GetExpoUsers();
                                }
                                if (listOfUsers != null)
                                {
                                    List<int> userIds = GetUserIdsByDeviceId(deviceId);
                                    var expoIds = listOfUsers.Where(x => userIds.Contains(x.UserId)).Select(x => x.ExpoId).Distinct().ToList();
                                    var bodySentence = $"{allList.FirstOrDefault(x => x.DeviceId == deviceId).CarPlateNumber} plakalı aracın kontağı kapandı.\nTarih: {DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss")}";//\nKonum: {allList.FirstOrDefault(x => x.DeviceId == deviceId).Location}

                                    SendPushNotification(expoIds.ToArray(), bodySentence, objs[0].Id);
                                }
                            }
                            else if (objs[0].Data != null && objs[0].Data.Substring(0,15).Contains("0001"))
                            {
                                if (listOfUsers == null)
                                {
                                    listOfUsers = GetExpoUsers();
                                }
                                List<int> userIds = GetUserIdsByDeviceId(deviceId);
                                if (listOfUsers != null)
                                {
                                    var expoIds = listOfUsers.Where(x => userIds.Contains(x.UserId)).Select(x => x.ExpoId).Distinct().ToList();
                                    var bodySentence = $"{allList.FirstOrDefault(x => x.DeviceId == deviceId).CarPlateNumber} plakalı aracın kontağı açıldı.\nTarih: {DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss")}";//\nKonum: {allList.FirstOrDefault(x => x.DeviceId == deviceId).Location}

                                    SendPushNotification(expoIds.ToArray(), bodySentence, objs[0].Id);
                                }
                            }
                            else if (objs[0].Data != null && objs[0].Data.Substring(0, 15).Contains("0000"))
                            {
                                if (listOfUsers == null)
                                {
                                    listOfUsers = GetExpoUsers();
                                }
                                if (listOfUsers != null)
                                {
                                    List<int> userIds = GetUserIdsByDeviceId(deviceId);
                                    var expoIds = listOfUsers.Where(x => userIds.Contains(x.UserId)).Select(x => x.ExpoId).Distinct().ToList();
                                    var bodySentence = $"{allList.FirstOrDefault(x => x.DeviceId == deviceId).CarPlateNumber} plakalı aracın kontağı kapandı.\nTarih: {DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss")}";//\nKonum: {allList.FirstOrDefault(x => x.DeviceId == deviceId).Location}

                                    SendPushNotification(expoIds.ToArray(), bodySentence, objs[0].Id);
                                }
                            }
                        }
                    }
                }
            }


            catch (Exception exc)
            {

                throw exc;
            }
            listOfUsers = null;
        }
        public static List<ExpoUser> GetExpoUsers()
        {
            try
            {
                using (var con = new SqlConnection(connectionString))
                {
                    if (con.State != ConnectionState.Open)
                        con.Open();
                    using (var cmd = con.CreateCommand())
                    {
                        string query = "SELECT * FROM UserExpo FOR JSON AUTO";

                        cmd.CommandText = query;

                        var reader = cmd.ExecuteReader();

                        StringBuilder sb = new StringBuilder();

                        while (reader.Read())
                            sb.Append(reader.GetString(0));

                        var json = sb.ToString();
                        var listOfUsers = JsonConvert.DeserializeObject<List<ExpoUser>>(json);
                        if (listOfUsers != null && listOfUsers.Count > 0)
                            return listOfUsers;
                    }
                    return null;
                }
            }
            catch (Exception exc)
            {
                throw exc;
            }
        }
        public static List<Device> GetUserDevices(int _userId)
        {
            try
            {
                using (var con = new SqlConnection(connectionString))
                {
                    if (con.State != ConnectionState.Open)
                        con.Open();
                    using (var cmd = con.CreateCommand())
                    {
                        string query = $"SELECT DeviceId FROM UserDevice WHERE UserId={_userId}";

                        cmd.CommandText = query;

                        var reader = cmd.ExecuteReader();

                        StringBuilder sb = new StringBuilder();

                        while (reader.Read())
                            sb.Append(reader.GetString(0));

                        var json = sb.ToString();
                        var listOfUsers = JsonConvert.DeserializeObject<List<Device>>(json);
                        if (listOfUsers != null || listOfUsers.Count > 0)
                            return listOfUsers;
                    }
                    return null;
                }
            }
            catch (Exception exc)
            {

                throw exc;
            }
        }
        public static List<DeviceStatus> GetAllDeviceStatus(List<int> _deviceIds)
        {
            try
            {
                using (var con = new SqlConnection(connectionString))
                {
                    if (con.State != ConnectionState.Open)
                        con.Open();
                    using (var cmd = con.CreateCommand())
                    {
                        var list = new List<string>();
                        var date = DateTime.Now.AddMinutes(-10).ToString("yyyy-MM-dd HH:mm:ss").Replace(".", "-");
                        foreach (var deviceId in _deviceIds)
                        {
                            list.Add($@"SELECT * FROM (SELECT Id,DeviceId,Data
                                        FROM DeviceDataAction
                                        WHERE DeviceId = {deviceId} and ActionType='Bildirim' and IsSendNotification=0 and Date>'{date}' ) AS T"); //and Date>'{date}'
                        }

                        var query = string.Join(" \nUNION ALL\n ", list);
                        query += " FOR JSON AUTO";

                        cmd.CommandText = query;
                        cmd.CommandTimeout = 0;
                        cmd.ExecuteNonQuery();

                        var reader = cmd.ExecuteReader();

                        StringBuilder sb = new StringBuilder();

                        while (reader.Read())
                            sb.Append(reader.GetString(0));

                        var json = sb.ToString();
                        if (json == "")
                        {
                            return new List<DeviceStatus>();
                        }
                        var listOfUsers = JsonConvert.DeserializeObject<List<DeviceStatus>>(json);
                        if (listOfUsers != null || listOfUsers.Count > 0)
                            return listOfUsers;
                    }
                    return null;
                }
            }
            catch (Exception exc)
            {
                throw exc;
            }
        }
        public static List<Device> GetAllDistinctDevices()
        {
            try
            {
                using (var con = new SqlConnection(connectionString))
                {
                    if (con.State != ConnectionState.Open)
                        con.Open();
                    using (var cmd = con.CreateCommand())
                    {
                        string query = $@"SELECT
	                                          (SELECT LastDeviceDataId FROM Device WHERE Id = T.DeviceId) AS LastDeviceDataId,
                                              (SELECT Latitude FROM DeviceData WHERE Id = (SELECT LastDeviceDataId FROM Device WHERE Id = T.DeviceId)) AS Latitude,
                                              (SELECT Longtitude FROM DeviceData WHERE Id = (SELECT LastDeviceDataId FROM Device WHERE Id = T.DeviceId)) AS Longtitude,
                                              (SELECT CarPlateNumber FROM Device WHERE Id = T.DeviceId) AS CarPlateNumber,
                                              * FROM
                                              (
                                                  SELECT DISTINCT DeviceId FROM DeviceData
                                              ) AS T FOR JSON AUTO";
                        cmd.CommandText = query;

                        var reader = cmd.ExecuteReader();

                        StringBuilder sb = new StringBuilder();

                        while (reader.Read())
                            sb.Append(reader.GetString(0));

                        var json = sb.ToString();
                        var listOfDevices = JsonConvert.DeserializeObject<List<Device>>(json);
                        if (listOfDevices != null || listOfDevices.Count > 0)
                            return listOfDevices;
                    }
                    return null;
                }
            }
            catch (Exception exc)
            {
                return new List<Device>();
            }
        }

        public static void SendPushNotification(string[] ExpoTokens, string bodySentence, int DDActionId)
        {
            foreach (var item in ExpoTokens)
            {
                try
                {
                    dynamic body = new
                    {
                        to = item,
                        title = "VeraMobil",
                        body = bodySentence,
                        sound = "default",
                    };
                    string response = null;
                    using (WebClient client = new WebClient())
                    {
                        //client.Encoding = System.Text.Encoding.UTF8;
                        client.Headers.Add("accept", "application/json");
                        client.Headers.Add("accept-encoding", "gzip, deflate");
                        client.Headers.Add("Content-Type", "application/json");
                        response = client.UploadString("https://exp.host/--/api/v2/push/send", JsonConvert.SerializeObject(body));
                        Console.WriteLine(response);
                    }
                    var result = JsonConvert.DeserializeObject<dynamic>(response);
                }
                catch (Exception exc)
                {
                    Console.WriteLine(exc.Message);
                }
            }

            using (var con = new SqlConnection(connectionString))
            {
                if (con.State != ConnectionState.Open)
                    con.Open();
                using (var cmd = con.CreateCommand())
                {
                    string query = $@"UPDATE DeviceDataAction  SET IsSendNotification=1 where Id={DDActionId}";
                    cmd.CommandText = query;
                    cmd.ExecuteNonQuery();
                }
            }
        }
        public static List<int> GetUserIdsByDeviceId(int _deviceId)
        {
            try
            {
                var userIds = new List<int>();
                using (var con = new SqlConnection(connectionString))
                {
                    if (con.State != ConnectionState.Open)
                        con.Open();
                    using (var cmd = con.CreateCommand())
                    {
                        string query = $"SELECT * FROM (SELECT UserId FROM UserDevice WHERE DeviceId = {_deviceId} UNION ALL SELECT UserId FROM CompanyUser WHERE CompanyId=(SELECT CompanyId FROM Device WHERE Id={_deviceId}) ) AS T FOR JSON AUTO";

                        cmd.CommandText = query;

                        var reader = cmd.ExecuteReader();

                        StringBuilder sb = new StringBuilder();

                        while (reader.Read())
                            sb.Append(reader.GetString(0));

                        var json = sb.ToString();
                        userIds = JsonConvert.DeserializeObject<List<UserTemp>>(json).Select(x => x.UserId).ToList();
                        return userIds;
                    }
                }
            }
            catch (Exception exc)
            {

                throw exc;
            }
        }
        public static DeviceData GetDeviceDataByDeviceDataId(int _deviceDataId)
        {
            try
            {
                using (var con = new SqlConnection(connectionString))
                {
                    if (con.State != ConnectionState.Open)
                        con.Open();
                    using (var cmd = con.CreateCommand())
                    {
                        string query = $"SELECT * FROM DeviceData WHERE Id = {_deviceDataId} FOR JSON AUTO";
                        cmd.CommandText = query;
                        var reader = cmd.ExecuteReader();

                        StringBuilder sb = new StringBuilder();

                        while (reader.Read())
                            sb.Append(reader.GetString(0));

                        var json = sb.ToString();
                        var deviceData = JsonConvert.DeserializeObject<List<DeviceData>>(json).FirstOrDefault();
                        return deviceData;
                    }
                }
            }
            catch (Exception exc)
            {

                throw exc;
            }
        }
        public static void InsertNewDeviceDataToNotGetNotificationAnymore(int _deviceDataId, string _ioStatus)
        {
            try
            {
                using (var con = new SqlConnection(connectionString))
                {
                    var deviceData = GetDeviceDataByDeviceDataId(_deviceDataId);
                    if (con.State != ConnectionState.Open)
                        con.Open();
                    using (var cmd = con.CreateCommand())
                    {
                        string query = $@"INSERT INTO DeviceData  
                                                       ([MessageType]
                                                       ,[DeviceId]
                                                       ,[IoStatus]
                                                       ,[Latitude]
                                                       ,[Longtitude]
                                                       ,[KmPerHour]
                                                       ,[TotalKm]
                                                       ,[DistanceBetweenTwoPackages]
                                                       ,[DirectionDegree]
                                                       ,[GpsDateTime]
                                                       ,[Altitude]
                                                       ,[CreateDate])
                                                 VALUES
                                                       ('{deviceData.MessageType}'
                                                       ,{deviceData.DeviceId}
                                                       ,'{_ioStatus}'
                                                       ,{deviceData.Latitude.ToString().Replace(",", ".")}
                                                       ,{deviceData.Longtitude.ToString().Replace(",", ".")}
                                                       ,{deviceData.KmPerHour}
                                                       ,{deviceData.TotalKm} 
                                                       ,{deviceData.DistanceBetweenTwoPackages}
                                                       ,{deviceData.DirectionDegree.ToString().Replace(",", ".")}
                                                       ,GETDATE()
                                                       ,{deviceData.Altitude.ToString().Replace(",", ".")}
                                                       ,GETDATE()) ";
                        cmd.CommandText = query;
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception exc)
            {

                throw exc;
            }
        }
    }
    public class UserTemp
    {
        public int UserId { get; set; }
    }
    public class ExpoUser
    {
        public int Id { get; set; }
        public int UserId { get; set; }
        public string ExpoId { get; set; }
    }
    public class Device
    {
        public int DeviceId { get; set; }
        public string CarPlateNumber { get; set; }
        public string Location { get; set; }
        public int LastDeviceDataId { get; set; }
    }
    public class DeviceStatus
    {
        public int Id { get; set; }
        public int DeviceId { get; set; }
        public string Data { get; set; }
    }
    public class DeviceData
    {
        public int Id { get; set; }

        public string MessageType { get; set; }

        public int DeviceId { get; set; }

        public string IoStatus { get; set; }

        public decimal Latitude { get; set; }

        public decimal Longtitude { get; set; }

        public int KmPerHour { get; set; }

        public int TotalKm { get; set; }

        public int DistanceBetweenTwoPackages { get; set; }

        public decimal DirectionDegree { get; set; }

        public DateTime GpsDateTime { get; set; }

        public decimal Altitude { get; set; }

        public DateTime CreateDate { get; set; }
    }

}
