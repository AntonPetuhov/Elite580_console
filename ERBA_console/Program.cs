using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.Net;
using System.Threading;
using System.Text.RegularExpressions;
using System.Data.SqlClient;
using System.Configuration;

namespace ERBA_console
{
    class Program
    {
        #region Настройки
        public static string IPadress = "172.18.95.31";  // сеть приборов cgm-app12
        public static int port = 8019;                   // используемый порт
        public static string AnalyzerCode = "904";       //код из аналайзер конфигурейшн, который связывает прибор в PSMV2
        public static string AnalyzerConfigurationCode = "ELITE580"; //код прибора из аналайзер конфигурейшн

        public static string user = "PSMExchangeUser"; //логин для базы обмена файлами и для базы CGM Analytix
        public static string password = "PSM_123456"; //пароль для базы обмена файлами и для базы CGM Analytix     

        public static bool ServiceIsActive = true;       //флаг для запуска и остановки потоков
        public static string AnalyzerResultPath = AppDomain.CurrentDomain.BaseDirectory + "\\AnalyzerResults"; // папка для файлов с результатами
        public static bool FileToErrorPath;              // флаг для перемещения файлов в ошибки или архив

        static object ExchangeLogLocker = new object();  // локер для логов обмена
        static object FileResultLogLocker = new object();  // локер для логов обмена
        static object ServiceLogLocker = new object();     //локер для логов драйвера

        public static int ExchangeTimeOut = 10;
        public static int CloseConnectionTimeOut = 5000;

        // управляющие биты
        static byte[] VT = { 0x0B };
        static byte[] FS = { 0x1C };
        static byte[] CR = { 0x0D };

        #endregion

        #region Функции логов

        // лог обмена с анализатором
        static void ExchangeLog(string Message)
        {
            lock (ExchangeLogLocker)
            {
                string path = AppDomain.CurrentDomain.BaseDirectory + "\\Log\\Exchange";
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }

                string filename = path + "\\ExchangeThread_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
                if (!System.IO.File.Exists(filename))
                {
                    using (StreamWriter sw = System.IO.File.CreateText(filename))
                    {
                        sw.WriteLine(DateTime.Now + ": " + Message);
                    }
                }
                else
                {
                    using (StreamWriter sw = System.IO.File.AppendText(filename))
                    {
                        sw.WriteLine(DateTime.Now + ": " + Message);
                    }
                }

            }
        }

        // Лог записи результатов в CGM
        static void FileResultLog(string Message)
        {
            try
            {
                lock (FileResultLogLocker)
                {
                    //string path = AppDomain.CurrentDomain.BaseDirectory + "\\Log\\FileResult" + "\\" + DateTime.Now.Year + "\\" + DateTime.Now.Month;
                    string path = AppDomain.CurrentDomain.BaseDirectory + "\\Log\\FileResult";
                    if (!Directory.Exists(path))
                    {
                        Directory.CreateDirectory(path);
                    }

                    //string filename = path + $"\\{FileName}" + ".txt";
                    string filename = path + $"\\ResultLog_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";

                    if (!System.IO.File.Exists(filename))
                    {
                        using (StreamWriter sw = System.IO.File.CreateText(filename))
                        {
                            sw.WriteLine(DateTime.Now + ": " + Message);
                        }
                    }
                    else
                    {
                        using (StreamWriter sw = System.IO.File.AppendText(filename))
                        {
                            sw.WriteLine(DateTime.Now + ": " + Message);
                        }
                    }
                }
            }
            catch
            {

            }
        }

        // Лог драйвера
        static void ServiceLog(string Message)
        {
            lock (ServiceLogLocker)
            {
                try
                {
                    string path = AppDomain.CurrentDomain.BaseDirectory + "\\Log\\Service";
                    if (!Directory.Exists(path))
                    {
                        Directory.CreateDirectory(path);
                    }

                    string filename = path + "\\ServiceThread_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
                    if (!System.IO.File.Exists(filename))
                    {
                        using (StreamWriter sw = System.IO.File.CreateText(filename))
                        {
                            sw.WriteLine(DateTime.Now + ": " + Message);
                        }
                    }
                    else
                    {
                        using (StreamWriter sw = System.IO.File.AppendText(filename))
                        {
                            sw.WriteLine(DateTime.Now + ": " + Message);
                        }
                    }
                }
                catch
                {

                }
            }
        }

        #endregion

        #region Функции
        //дописываем к номеру месяца ноль если нужно
        public static string CheckZero(int CheckPar)
        {
            string BackPar = "";
            if (CheckPar < 10)
            {
                BackPar = $"0{CheckPar}";
            }
            else
            {
                BackPar = $"{CheckPar}";
            }
            return BackPar;
        }

        //собираем несколько массивов в один
        static Byte[] ConcatByteArray(params Byte[][] ArraysPar)
        {
            Byte[] FinallArray = { };
            for (int i = 0; i < ArraysPar.Length; i++)
            {
                int EndOfGeneralArray = FinallArray.Length;
                Array.Resize(ref FinallArray, FinallArray.Length + ArraysPar[i].Length);
                Array.Copy(ArraysPar[i], 0, FinallArray, EndOfGeneralArray, ArraysPar[i].Length);
            }
            return FinallArray;
        }

        // Создаем файл с результатом, отправленным анализатором
        static void MakeAnalyzerResultFile(string AllMessagePar)
        {
            if (!Directory.Exists(AnalyzerResultPath))
            {
                Directory.CreateDirectory(AnalyzerResultPath);
            }
            DateTime now = DateTime.Now;
            string filename = AnalyzerResultPath + "\\Results_" + now.Year + CheckZero(now.Month) + CheckZero(now.Day) + CheckZero(now.Hour) + CheckZero(now.Minute) + CheckZero(now.Second) + CheckZero(now.Millisecond) + ".res";
            using (System.IO.FileStream fs = new System.IO.FileStream(filename, FileMode.OpenOrCreate))
            {
                foreach (string res in AllMessagePar.Split('\r'))
                {
                    byte[] ResByte = Encoding.GetEncoding(1251).GetBytes(res + "\r\n");
                    fs.Write(ResByte, 0, ResByte.Length);
                }
            }
        }
        #endregion

        #region Преобразование кодов тестов
        // Функция преобразования кода теста прибора в код теста PSMV2 в CGM
        public static string TranslateToPSMCodes(string AnalyzerTestCodesPar)
        {
            //FileResultLog(AnalyzerTestCodesPar);
            ///*
            string BackTestCode = "";
            try
            {
                string CGMConnectionString = ConfigurationManager.ConnectionStrings["CGMConnection"].ConnectionString;
                //string CGMConnectionString = @"Data Source=CGM-APP11\SQLCGMAPP11;Initial Catalog=KDLPROD; Integrated Security=True;";
                CGMConnectionString = String.Concat(CGMConnectionString, $"User Id = {user}; Password = {password}");
                using (SqlConnection Connection = new SqlConnection(CGMConnectionString))
                {
                    Connection.Open();
                    //ищем RID в базе
                    SqlCommand TestCodeCommand = new SqlCommand(
                       "SELECT k1.amt_analyskod  FROM konvana k " +
                       "LEFT JOIN konvana k1 ON k1.met_kod = k.met_kod AND k1.ins_maskin = 'PSMV2' " +
                       $"WHERE k.ins_maskin = '{AnalyzerConfigurationCode}' AND k.amt_analyskod = '{AnalyzerTestCodesPar}' ", Connection);
                    SqlDataReader Reader = TestCodeCommand.ExecuteReader();

                    if (Reader.HasRows) // если есть данные
                    {
                        //FileResultLog(AnalyzerTestCodesPar);
                        while (Reader.Read())
                        {
                            if (!Reader.IsDBNull(0)) { BackTestCode = Reader.GetString(0); };
                        }
                        //FileResultLog(BackTestCode);
                    }
                    Reader.Close();
                    Connection.Close();
                }
            }
            catch (Exception error)
            {
                FileResultLog($"Error: {error}");
                //Console.WriteLine("NO test interpretation");
            }
            //Console.WriteLine("EMPTY test code");
            return BackTestCode;
            //*/
            /*
            switch (AnalyzerTestCodesPar)
            {
                // WBC
                //case "6690-2":
                case "WBC":
                    return "0301";
                // RBC
                //case "789-8":
                case "RBC":
                    return "0302";
                // HGB
                //case "718-7":
                case "HGB":
                    return "0303";
                default:
                    return "";
            }
            */
        }

        // Функция преобразования кода теста CGM в код теста, понятный прибору 
        // не используется, т.к. прибору посылаем код метода CBC или CBC+DIFF
        public static string TranslateToAnalyzerCodes(string CGMTestCodesPar)
        {
            string BackTestCode = "";
            try
            {
                string CGMConnectionString = ConfigurationManager.ConnectionStrings["CGMConnection"].ConnectionString;
                CGMConnectionString = String.Concat(CGMConnectionString, $"User Id = {user}; Password = {password}");
                using (SqlConnection Connection = new SqlConnection(CGMConnectionString))
                {
                    Connection.Open();
                    //ищем RID в базе
                    SqlCommand TestCodeCommand = new SqlCommand(
                       "SELECT TOP 1 k.amt_analyskod  FROM konvana k " +
                       $"WHERE k.ins_maskin = '{AnalyzerConfigurationCode}' AND k.met_kod = '{CGMTestCodesPar}' ", Connection);
                    SqlDataReader Reader = TestCodeCommand.ExecuteReader();

                    if (Reader.HasRows) // если есть данные
                    {
                        while (Reader.Read())
                        {
                            if (!Reader.IsDBNull(0)) { BackTestCode = Reader.GetString(0); };
                        }
                    }
                    Reader.Close();
                    Connection.Close();
                }
            }
            catch (Exception error)
            {
                FileResultLog($"Error: {error}");
            }
            return BackTestCode;
        }

        #endregion

        #region Функция обработки файлов с результатами и создания файлов для службы, которая разберет файл и запишет данные в CGM
        static void ResultsProcessing()
        {
            while (ServiceIsActive)
            {
                try
                {
                    #region папки архива, результатов и ошибок

                    string OutFolder = ConfigurationManager.AppSettings["FolderOut"];
                    // архивная папка
                    string ArchivePath = AnalyzerResultPath + @"\Archive";
                    // папка для ошибок
                    string ErrorPath = AnalyzerResultPath + @"\Error";
                    // папка для файлов с результатами для CGM
                    string CGMPath = AnalyzerResultPath + @"\CGM";

                    if (!Directory.Exists(ArchivePath))
                    {
                        Directory.CreateDirectory(ArchivePath);
                    }

                    if (!Directory.Exists(ErrorPath))
                    {
                        Directory.CreateDirectory(ErrorPath);
                    }

                    if (!Directory.Exists(CGMPath))
                    {
                        Directory.CreateDirectory(CGMPath);
                    }
                    #endregion

                    // строки для формирования файла (psm файла) с результатами для службы,
                    // которая разбирает файлы и записывает результаты в CGM
                    string MessageHead = "";
                    string MessageTest = "";
                    string AllMessage = "";

                    // поолучаем список всех файлов в текущей папке
                    string[] Files = Directory.GetFiles(AnalyzerResultPath, "*.res");

                    // шаблоны регулярных выражений для поиска данных
                    string RIDPattern = @"OBR[|][1][|]{2}(?<RID>\d+)[|]{1}\S*";
                    string TestPattern = @"OBX[|]\d+[|]NM[|]\S+[@](?<Test>\S+)[@]\S*";
                    string ResultPattern = @"OBX[|]\d+[|]NM[|]\S+[|](?<Result>\d+[.]?\d*)[|]\S+";

                    Regex RIDRegex = new Regex(RIDPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                    Regex TestRegex = new Regex(TestPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                    Regex ResultRegex = new Regex(ResultPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));

                    // пробегаем по файлам
                    foreach (string file in Files)
                    {
                        Console.WriteLine(file);
                        FileResultLog(file);
                        string[] lines = System.IO.File.ReadAllLines(file);
                        string RID = "";
                        string Test = "";
                        string Result = "";

                        // обрезаем только имя текущего файла
                        string FileName = file.Substring(AnalyzerResultPath.Length + 1);
                        // название файла .ок, который должен создаваться вместе с результирующим для обработки службой FileGetterService
                        string OkFileName = "";

                        // проходим по строкам в файле
                        foreach (string line in lines)
                        {
                            // заменяем птички ^ на @, иначе регулярное врыажение некорректно работает
                            string line_ = line.Replace("^", "@");
                            Match RIDMatch = RIDRegex.Match(line_);
                            Match TestMatch = TestRegex.Match(line_);
                            Match ResultMatch = ResultRegex.Match(line_);

                            // поиск RID в строке
                            if (RIDMatch.Success)
                            {
                                RID = RIDMatch.Result("${RID}");
                                Console.WriteLine(RID);
                                FileResultLog($"Reguistion № {RID}");
                                MessageHead = $"O|1|{RID}||ALL|R|20230101000100|||||X||||ALL||||||||||F";
                            }
                            else
                            {
                                //Console.WriteLine("RID не найден в строке");
                                //FileResultLog("RID не найден");
                                //FileToErrorPath = true;
                            }

                            // поиск теста в строке
                            if (TestMatch.Success)
                            {
                                Test = TestMatch.Result("${Test}");
                                // преобразуем тест в код теста PSM
                                string PSMTestCode = TranslateToPSMCodes(Test);
                                
                                //Console.WriteLine(PSMTestCode);
                                
                                //Console.WriteLine(Test);
                                if (ResultMatch.Success)
                                {
                                    Result = ResultMatch.Result("${Result}");
                                    //Console.WriteLine($"{Test} - result: {Result}");
                                }

                                // если код тест был интерпретирован
                                if (PSMTestCode != "")
                                {
                                    // формируем строку с ответом для результирующего файла
                                    MessageTest = MessageTest + $"R|1|^^^{PSMTestCode}^^^^{AnalyzerCode}|{Result}|||N||F||ErbaElite^||20230101000001|{AnalyzerCode}" + "\r";
                                    //Console.WriteLine(MessageTest);
                                }
                            }
                        }

                        // получаем название файла .ок на основании файла с результатом
                        if (FileName.IndexOf(".") != -1)
                        {
                            OkFileName = FileName.Split('.')[0] + ".ok";
                            //Console.WriteLine(OkFileName);
                        }

                        // если строки с результатами и с ШК не пустые, значит формируем результирующий файл
                        if (MessageHead != "" && MessageTest != "")
                        {
                            try
                            {
                                // собираем полное сообщение с результатом
                                AllMessage = MessageHead + "\r" + MessageTest;
                                //Console.WriteLine(AllMessage);
                                FileResultLog(AllMessage);

                                // создаем файл для записи результата в папке для рез-тов
                                //if (!File.Exists(CGMPath + @"\" + FileName))
                                if (!File.Exists(OutFolder + @"\" + FileName))
                                {
                                    //using (StreamWriter sw = File.CreateText(CGMPath + @"\" + FileName))
                                    using (StreamWriter sw = File.CreateText(OutFolder + @"\" + FileName))
                                    {
                                        foreach (string msg in AllMessage.Split('\r'))
                                        {
                                            sw.WriteLine(msg);
                                        }
                                    }
                                }
                                else
                                {
                                    //File.Delete(CGMPath + @"\" + FileName);
                                    //using (StreamWriter sw = File.CreateText(CGMPath + @"\" + FileName))
                                    File.Delete(OutFolder + @"\" + FileName);
                                    using (StreamWriter sw = File.CreateText(OutFolder + @"\" + FileName))
                                    {
                                        foreach (string msg in AllMessage.Split('\r'))
                                        {
                                            sw.WriteLine(msg);
                                        }
                                    }
                                }

                                // создаем .ok файл в папке для рез-тов
                                if (OkFileName != "")
                                {
                                    //if (!File.Exists(CGMPath + @"\" + OkFileName))
                                    if (!File.Exists(OutFolder + @"\" + OkFileName))
                                    {
                                        //using (StreamWriter sw = File.CreateText(CGMPath + @"\" + OkFileName))
                                        using (StreamWriter sw = File.CreateText(OutFolder + @"\" + OkFileName))
                                        {
                                            sw.WriteLine("ok");
                                        }
                                    }
                                    else
                                    {
                                        //File.Delete(CGMPath + OkFileName);
                                        //using (StreamWriter sw = File.CreateText(CGMPath + @"\" + OkFileName))
                                        File.Delete(OutFolder + OkFileName);
                                        using (StreamWriter sw = File.CreateText(OutFolder + @"\" + OkFileName))
                                        {
                                            sw.WriteLine("ok");
                                        }
                                    }
                                }

                                // помещение файла в архивную папку
                                if (File.Exists(ArchivePath + @"\" + FileName))
                                {
                                    File.Delete(ArchivePath + @"\" + FileName);
                                }
                                File.Move(file, ArchivePath + @"\" + FileName);

                                FileResultLog("Файл обработан и перемещен в папку Archive");
                                FileResultLog("");
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);
                                // помещение файла в папку с ошибками
                                if (File.Exists(ErrorPath + @"\" + FileName))
                                {
                                    File.Delete(ErrorPath + @"\" + FileName);
                                }
                                File.Move(file, ErrorPath + @"\" + FileName);
                                FileResultLog("Ошибка обработки файла. Файл перемещен в папку Error");
                                FileResultLog("");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    FileResultLog(ex.ToString());
                }
                Thread.Sleep(1000);
            }
        }
        #endregion

        #region Получение данных по заявке и задания, формирование строки с заданием в формате HL7
        
        public static string GetRequestFromCGMDB(string RIDPar)
        {
            // переменные для данных из CGM
            string PID = "";
            string PatientSurname = "";
            string PatientName = "";
            string PatientSex = "";
            string PatientBirthDay = "";
            string RegistrationDate = "";
            string SampleDate = "";
            DateTime PatientBirthDayDate = new DateTime();
            DateTime RegistrationDateDate = DateTime.Now;
            DateTime SampleDateDate = DateTime.Now;
            string test = "";
            bool RIDExists = false;
            bool DiffMethod = false;
            string TestMethod = "";
            string BackRequestString = "";

            string CGMConnectionString = ConfigurationManager.ConnectionStrings["CGMConnection"].ConnectionString;
            CGMConnectionString = String.Concat(CGMConnectionString, $"User Id = {user}; Password = {password}");

            //Сначала получаем необходимые данные
            try
            {
                using (SqlConnection CGMconnection = new SqlConnection(CGMConnectionString))
                {
                    CGMconnection.Open();

                    //ищем RID в базе
                    SqlCommand RequetDataCommand = new SqlCommand(
                       "SELECT TOP 1" +
                         "p.pop_pid AS PID, p.pop_enamn AS PatientSurname, p.pop_fnamn AS PatientName, p.pop_fdatum AS PatientBirthday, " +
                         "CASE WHEN p.pop_kon = 'K' THEN 'Female' ELSE 'Male' END AS PatientSex, " +
                         "r.rem_ank_dttm AS RegistrationDate " +
                       "FROM dbo.remiss (NOLOCK) r " +
                         "INNER JOIN dbo.pop (NOLOCK) p ON p.pop_pid = r.pop_pid " +
                       "WHERE r.rem_deaktiv = 'O' " +
                         $"AND r.rem_rid IN ('{RIDPar}') " +
                         "AND r.rem_ank_dttm IS NOT NULL ", CGMconnection);
                    SqlDataReader Reader = RequetDataCommand.ExecuteReader();
                    // если такой ШК есть
                    if (Reader.HasRows)
                    {
                        RIDExists = true;
                        // получаем данные по заявке
                        while (Reader.Read())
                        {
                            if (!Reader.IsDBNull(0)) { PID = Reader.GetString(0); };
                            if (!Reader.IsDBNull(1)) { PatientSurname = Reader.GetString(1); };
                            if (!Reader.IsDBNull(2)) { PatientName = Reader.GetString(2); };
                            if (!Reader.IsDBNull(3))
                            {
                                PatientBirthDayDate = Reader.GetDateTime(3);
                                PatientBirthDay = PatientBirthDayDate.Year + CheckZero(PatientBirthDayDate.Month) + CheckZero(PatientBirthDayDate.Day);
                            }
                            if (!Reader.IsDBNull(4)) { PatientSex = Reader.GetString(4); };
                          
                            if (!Reader.IsDBNull(5))
                            {
                                RegistrationDateDate = Reader.GetDateTime(5);
                            };
                        }
                    }
                    Reader.Close();
                    //CGMconnection.Close();

                    if (RIDExists)
                    {
                        SqlCommand TestCodeCommand = new SqlCommand(
                            "SELECT b.ana_analyskod, prov.pro_provdat " +
                            "FROM dbo.remiss (NOLOCK) r " +
                              "INNER JOIN dbo.bestall (NOLOCK) b ON b.rem_id = r.rem_id " +
                              "INNER JOIN dbo.prov (NOLOCK) prov ON prov.pro_id = b.pro_id " +
                            "WHERE r.rem_deaktiv = 'O' " +
                            $"AND r.rem_rid IN('{RIDPar}') " +
                            "AND r.rem_ank_dttm IS NOT NULL", CGMconnection);
                        SqlDataReader TestsReader = TestCodeCommand.ExecuteReader();
                        
                        if (TestsReader.HasRows)
                        {
                            Console.WriteLine("задания есть");
                            while (TestsReader.Read())
                            {

                                //test = TestsReader.GetString(0);
                                //Console.WriteLine(test);
                                if (!TestsReader.IsDBNull(0))
                                {
                                    test = TestsReader.GetString(0);
                                    //Console.WriteLine(test);

                                    // если тест LYMPH, то должно быть передано задание на CBC+DIFF
                                    if ((test == "Г0080") || (test == "Г0080"))
                                    {
                                        DiffMethod = true;
                                    }
                                }
                                // Sample date from prov table
                                SampleDateDate = TestsReader.GetDateTime(1);
                            }
                        }
                        TestsReader.Close();
                    }
                    CGMconnection.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            #region Формируем HL7 с заданием
            // Формируем HL7 с заданием
            RegistrationDate = RegistrationDateDate.Year + CheckZero(RegistrationDateDate.Month) + CheckZero(RegistrationDateDate.Day);
            SampleDate = SampleDateDate.Year + CheckZero(SampleDateDate.Month) + CheckZero(SampleDateDate.Day) + CheckZero(SampleDateDate.Hour) 
                        + CheckZero(SampleDateDate.Minute) + CheckZero(SampleDateDate.Second);
            if (PatientSurname.Length > 20) { PatientSurname = PatientSurname.Substring(0, 20); }
            if (PatientName.Length > 28) { PatientName = PatientName.Substring(0, 28); }

            if (DiffMethod == true)
            {
                TestMethod = "CBC+DIFF";
                
            };
            if (DiffMethod == false)
            {
                TestMethod = "CBC";
            };

            Console.WriteLine(TestMethod);
            //else
            //{
            //   TestMethod = "CBC";
            //}

            // шаблон ответа с заданием в формате HL7 (по мануалу) без блоков MSH и MSA
            string taskPID = $@"PID|{PID}||{PID}^^^^MR||{PatientSurname}^{PatientName}||{PatientBirthDay}000000|{PatientSex}";
            string taskPV1 = $@"PV1|1";
            //string taskPV1 = "PV1|1|Inpatient|Surgical^1^2|||||||||||||||||Self-paid";
            string taskORC = $@"ORC|AF|{RIDPar}|||";
            string taskOBR = $@"OBR|1|{RIDPar}||01001^Automated Count^99MRC||{SampleDate}||||||||{RegistrationDate}||||||||||HM||||||||";
            string taskOBX = $@"OBX|1|IS|02001^Take Mode^99MRC||O||||||" + '\r' +
                             $@"OBX|2|IS|02002^Blood Mode^99MRC||W||||||" + '\r' +
                             $@"OBX|3|IS|02003^Test Mode^99MRC||{TestMethod}||||||";

            BackRequestString = taskPID + '\r' + taskPV1 + '\r' + taskORC + '\r' + taskOBR + '\r' + taskOBX;
            return BackRequestString;
            #endregion
        }

        #endregion

        // TCP server
        static void TCPServer()
        {
            try
            {
                while (ServiceIsActive)
                {
                    IPAddress ip = IPAddress.Parse(IPadress);
                    // локальная точка EndPoint, на которой сокет будет принимать подключения от клиентов
                    EndPoint endpoint = new IPEndPoint(ip, port);
                    // создаем сокет
                    Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    // связываем сокет с локальной точкой endpoint 
                    socket.Bind(endpoint);

                    // получаем конечную точку, с которой связан сокет
                    Console.WriteLine(socket.LocalEndPoint);
                    ServiceLog(socket.LocalEndPoint.ToString());

                    // запуск прослушивания подключений
                    socket.Listen(1000);
                    Console.WriteLine("TCP Сервер запущен. Ожидание подключений...");
                    ServiceLog("TCP Сервер запущен. Ожидание подключений...");
                    // После начала прослушивания сокет готов принимать подключения
                    // получаем входящее подключение
                    Socket client = socket.Accept();

                    // получаем адрес клиента, который подключился к нашему tcp серверу
                    Console.WriteLine($"Адрес подключенного клиента: {client.RemoteEndPoint}");
                    ServiceLog($"Адрес подключенного клиента: {client.RemoteEndPoint}");

                    int ServerCount = 0; // счетчик

                    while (ServiceIsActive)
                    {
                        // состояние сокета
                        // client.Poll(1, SelectMode.SelectRead) - true, если:
                        // если был вызван метод Listen(Int32) и подключение отложено
                        // если данные доступны для чтения
                        // если подключение закрыто, сброшено или завершено
                        // Console.WriteLine($"handler.Available {client.Available}; " +
                        //   $"SelectRead: {client.Poll(1, SelectMode.SelectRead)};" +
                        //   $"SelectWrite: {client.Poll(1, SelectMode.SelectWrite)};" +
                        //   $"SelectError: {client.Poll(1, SelectMode.SelectError)};");

                        // нет данных для чтения и соединение не активно
                        if (client.Poll(1, SelectMode.SelectRead) && client.Available == 0)
                        {
                            //CloseConnectionForcely = false;
                            client = socket.Accept();
                            ServiceLog("Ожидание переподключения");
                        }

                        #region принудительное закрытие сокета (не используется)
                        /*
                        // принудительное закрытие сокета
                        if (CloseSocket)
                        {
                            CloseSocket= false;
                            ServiceLog("Перезагрузка сокета");
                            client.Shutdown(SocketShutdown.Both);
                            client.Close();
                            client = socket.Accept();
                        }
                        */
                        #endregion

                        // если клиент ничего не посылает
                        if (client.Available == 0)
                            {
                            ServerCount++;
                            if (ServerCount == 100)
                            {
                                ServerCount = 0;
                                Console.WriteLine("Прослушивание сокета...");
                                // состояние сокета
                                ServiceLog($"handler.Available {client.Available}; " +
                                           $"SelectRead: {client.Poll(1, SelectMode.SelectRead)};" +
                                           $"SelectWrite: {client.Poll(1, SelectMode.SelectWrite)};" +
                                           $"SelectError: {client.Poll(1, SelectMode.SelectError)};");
                                ServiceLog("Прослушивание сокета...");
                                ServiceLog("");
                            }
                        }
                        // получаем сообщение от анализатора
                        else
                        {
                            // UTF8 encoder
                            Encoding utf8 = Encoding.UTF8;
                            // StringBuilder для склеивания полученных данных в одну строку
                            // var messageFromElite = new StringBuilder();
                            // количество полученных байтов
                            // int received_bytes = 0; 
                            // буфер для получения данных
                            // byte[] received_data = new byte[4096];
                            // буфер для считывания одного байта, т.к. читать будем по байтам, чтобы найти байт окончания ответа
                            var bytesRead = new byte[1];
                            // буфер для накопления входящих данных
                            var buffer = new List<byte>();

                            #region чтение сообщения от прибора (тоже рабочий вариант)
                            //Смотрим, сколько байтов и считываем пока клиент отправляет 
                            /*
                             // записывает в messageFromElite целиком сообщение от прибора, 
                             // и если несколько заявок сразу выгружается, то все в одном сообщении
                             // как вариант - резать сообщения
                            do
                            {
                                received_bytes = client.Receive(received_data);
                                // GetString - декодирует последовательность байтов из указанного массива байтов в строку.
                                // преобразуем полученный набор байтов в строку
                                string ResponseMsg = Encoding.UTF8.GetString(received_data, 0, received_bytes);
                                //messageFromElite.Append(Encoding.UTF8.GetString(received_data, 0, received_bytes));

                                // добавляем в StringBuilder
                                messageFromElite.Append(ResponseMsg);
                                ExchangeLog(messageFromElite.ToString());
                            }
                            while (client.Available > 0);

                            // записываем сообщение от прибора в лог
                            ExchangeLog(messageFromElite.ToString());
                            */
                            #endregion
                            // состояние сокета
                            ServiceLog($"handler.Available {client.Available}; " +
                                           $"SelectRead: {client.Poll(1, SelectMode.SelectRead)};" +
                                           $"SelectWrite: {client.Poll(1, SelectMode.SelectWrite)};" +
                                           $"SelectError: {client.Poll(1, SelectMode.SelectError)};");
                            ServiceLog("Получение сообщения от анализатора");
                            ServiceLog("");
                            // читаем по одному байту
                            while (true)
                            {
                                var bytesCount = client.Receive(bytesRead);

                                // смотрим, если считанный байт представляет конечный символ, выходим
                                if (bytesCount == 0 || bytesRead[0] == FS[0]) break;
                                // если символ начала сообщения
                                if (bytesRead[0] == VT[0])
                                //if (bytesRead[0] == VT[0] || bytesRead[0] == CR[0])
                                {
                                    // не добавляем в результирующий массив
                                    // Console.WriteLine("VT byte");
                                }
                                else
                                {
                                    // иначе добавляем в буфер
                                    buffer.Add(bytesRead[0]);
                                }

                                // иначе добавляем в буфер
                                //buffer.Add(bytesRead[0]);
                            }

                            #region Считывание оставшегося байта на сокете
                            // На сокете остается байт, его нужно считать
                            if (client.Poll(1, SelectMode.SelectRead) && client.Available == 1)
                            {
                                Console.WriteLine("Есть байт на сокете, считаем его");
                                ServiceLog($"handler.Available {client.Available}; " +
                                                  $"SelectRead: {client.Poll(1, SelectMode.SelectRead)};" +
                                                  $"SelectWrite: {client.Poll(1, SelectMode.SelectWrite)};" +
                                                  $"SelectError: {client.Poll(1, SelectMode.SelectError)};");
                                ServiceLog("Есть байт на сокете. Считывание.");
                                ServiceLog("");
                                var byteFromSocket = new byte[1];
                                do
                                {
                                    var bytesFromSocketCount = client.Receive(byteFromSocket);
                                }
                                while (client.Available > 0);
                            }
                            #endregion

                            // сообщение от анализатора
                            var message = Encoding.UTF8.GetString(buffer.ToArray());
                            //ExchangeLog("Analyzer:");
                            //ExchangeLog(message);
                            ExchangeLog("Analyzer:" + "\n" + $"{message}");
                            // Создаем файлы с результатами в папке AnalyzerResults
                            //MakeAnalyzerResultFile(messageFromElite.ToString());

                            // проверяем тип сообщения, запрос задания или выгрузка результатов?
                            // нужно заменить птички, иначе рег.выражение не работает
                            string messageElite = message.ToString().Replace("^", "@");

                            #region Определение типа сообщения от прибора
                            // Тип сообщения ORM - запрос задания прибором
                            string ORMPattern = @"\S+[|](?<type>\w+)@O01[|]\w+[|]";
                            Regex ORMRegex = new Regex(ORMPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                            string ORM = "";

                            Match ORMMatch = ORMRegex.Match(messageElite);

                            if (ORMMatch.Success)
                            {
                                ORM = ORMMatch.Result("${type}");
                                //Console.WriteLine(ORM);
                                //ExchangeLog($"Message type: {ORM}");
                                //Console.WriteLine($"Message type: {ORM}");
                            }

                            // Тип сообщения ORU - сообщение с результатом
                            string ORUPattern = @"\S+[|](?<type>\w+)@R01[|]\w+[|]";
                            Regex ORURegex = new Regex(ORUPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                            string ORU = "";

                            Match ORUMatch = ORURegex.Match(messageElite);

                            if (ORUMatch.Success)
                            {
                                ORU = ORUMatch.Result("${type}");
                                //Console.WriteLine(ORU);
                                //ExchangeLog($"Message type: {ORU}");
                                //Console.WriteLine($"Message type: {ORU}");
                            }
                            #endregion

                            // Если запрос задания
                            if (ORM == "ORM")
                            {
                                // шаблона для поиска guid в сообщении от прибора
                                string guidPattern = @"\S+[|]ORM@O01[|](?<guid>\w+)[|]";
                                Regex guidRegex = new Regex(guidPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                                string guid = "";

                                Match guidMatch = guidRegex.Match(messageElite);

                                if (guidMatch.Success)
                                {
                                    guid = guidMatch.Result("${guid}");
                                    //Console.WriteLine(guid);
                                    //ExchangeLog($"find guid: {guid}");
                                }
                                else
                                {
                                    //Console.WriteLine("GUID not found!");
                                    //ExchangeLog($"GUID not found!");
                                }

                                // шаблона для поиска RID в сообщении от прибора
                                string RIDPattern = @"ORC[|]RF[|]+(?<RID>\d+)[|]\S+";
                                Regex RIDRegex = new Regex(RIDPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                                string RID = "";

                                Match RIDMatch = RIDRegex.Match(messageElite);

                                if (RIDMatch.Success)
                                {
                                    RID = RIDMatch.Result("${RID}");
                                    //Console.WriteLine(RID);
                                    //ExchangeLog($"RID: {RID}");
                                }
                                else
                                {
                                    //Console.WriteLine("RID not found!");
                                    //ExchangeLog($"RID not found!");
                                }

                                #region Формирование строки с заданием в формате HL7 (по мануалу)
                                /*
                                // шаблон ответа с заданием в формате HL7 (по мануалу)
                                string taskMSH = $@"MSH|^~\&|DH56|ERBA|||20230419101033||ORR^O02|{guid}|P|2.3.1||||||UNICODE";
                                string taskMSA = $@"MSA|AA|{guid}";
                                string taskPID = $@"PID|000001||05012006^^^^MR||^Test test||19991001000000|Male";
                                //string taskPV1 = $@"PV1|1";
                                string taskPV1 = "PV1|1|Inpatient|Surgical^1^2|||||||||||||||||Self-paid";
                                string taskORC = $@"ORC|AF|1000002687|||";
                                string taskOBR = $@"OBR|1|1000002687||01001^Automated Count^99MRC||20230419101927||||||||20230419101927||||||||||HM||||||||";
                                string taskOBX = $@"OBX|1|IS|02001^Take Mode^99MRC||O||||||" + '\r' +
                                                 $@"OBX|2|IS|02002^Blood Mode^99MRC||W||||||" + '\r' +
                                                 $@"OBX|3|IS|02003^Test Mode^99MRC||CBC+DIFF||||||" + '\r' +
                                                 $@"OBX|4|NM|30525-0^Age^LN||||||||" + '\r' +
                                                 $@"OBX|5|IS|09001^Remark^99MRC||||||||" + '\r' +
                                                 $@"OBX|6|IS|03001^Ref Group^99MRC||Общ.||||||";

                                string requestString = "";

                                requestString = taskMSH + '\r' + taskMSA + '\r' + taskPID + '\r' + taskPV1 + '\r' + taskORC + '\r' + taskOBR + '\r' + taskOBX;
                                */
                                #endregion

                                DateTime now = DateTime.Now;
                                string NowString = $"{now.Year + CheckZero(now.Month) + CheckZero(now.Day) + CheckZero(now.Hour) + CheckZero(now.Minute)}";

                                // шаблон ответа с заданием в формате HL7 (по мануалу)
                                string taskMSH = $@"MSH|^~\&|DH56|ERBA|||{now}||ORR^O02|{guid}|P|2.3.1||||||UNICODE";
                                string taskMSA = $@"MSA|AA|{guid}";

                                string requestString = "";

                                requestString = taskMSH + '\r' + taskMSA + '\r' + GetRequestFromCGMDB(RID);

                                // строка ответа с результатом
                                byte[] SendingMessageBytes;
                                SendingMessageBytes = ConcatByteArray(VT, utf8.GetBytes(requestString), FS, CR);

                                if (client.Poll(1, SelectMode.SelectWrite))
                                {
                                    // Отправка задания анализатору
                                    client.Send(SendingMessageBytes);
                                }

                                    // Отправка задания анализатору
                                    //client.Send(SendingMessageBytes);

                                Thread.Sleep(1000);

                                //ExchangeLog("LIS:");
                                //ExchangeLog(utf8.GetString(SendingMessageBytes));
                                ExchangeLog("LIS:" + "\n" + utf8.GetString(SendingMessageBytes));
                            }

                            // если сообщение с результатами - ORU
                            if (ORU == "ORU")
                            {
                                MakeAnalyzerResultFile(message);
                            }

                            //MakeAnalyzerResultFile(message);

                            #region попытка отправки ACK прибору (прибор выдавал таймаут)
                            // ACK sending
                            /*
                            // нужно заменить птички, иначе рег.выражение не работает
                            string line = messageFromElite.ToString().Replace("^", "@");

                            // шаблона для поиска guid в сообщении от прибора
                            string guidPattern = @"\S+[|]ORU@R01[|](?<guid>\w+)[|]";
                            Regex guidRegex = new Regex(guidPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                            string guid = "";
                            // шаблона для поиска даты в сообщении от прибора
                            string datePattern = @"\S+Erba[|]+(?<date>\d+)[|]+ORU@R01[|]";
                            Regex dateRegex = new Regex(datePattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                            string date = "";

                            Match guidMatch = guidRegex.Match(line);

                            if (guidMatch.Success)
                            {
                                guid = guidMatch.Result("${guid}");
                                Console.WriteLine(guid);
                                ExchangeLog($"find guid: {guid}");
                            }
                            else
                            {
                                ExchangeLog("guid find FAIL");
                            }

                            Match dateMatch = dateRegex.Match(line);
                            if (dateMatch.Success)
                            {
                                date = dateMatch.Result("${date}");
                                ExchangeLog($"find date: {date}");
                            }
                            else
                            {
                                ExchangeLog("Date find FAIL");
                            }

                            // шаблон ответа ACK в формате HL7 (по мануалу)
                            string ackMSH = $@"MSH|^~\&|DH56|ERBA|||{date}||ACK^R01|{guid}|P|2.3.1||||||UNICODE";
                            string ackMSA = $@"MSA|AA|{guid}";

                            // строка подтверждения
                            byte[] SendingMessageBytes;
                            SendingMessageBytes = ConcatByteArray(VT, utf8.GetBytes(ackMSH), CR, utf8.GetBytes(ackMSA), FS, CR);
                            //SendingMessageBytes = ConcatByteArray(VT, utf8.GetBytes(ackMSH), utf8.GetBytes(ackMSA), FS, CR);
                            //SendingMessageBytes = ConcatByteArray(VT, utf8.GetBytes(ackMSA), FS, CR);

                            if (received_data[0] == VT[0])
                            {
                                try
                                {
                                    //client.Send(ACK);
                                    client.Send(SendingMessageBytes);
                                    ExchangeLog($"S: Send ACK");
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine(e);
                                }
                            }
                            else
                            {
                                //client.Send(ACK);
                                client.Send(SendingMessageBytes);
                                //client.Send(ACK);
                                ExchangeLog($"Server send <ACK>");
                                ExchangeLog(utf8.GetString(SendingMessageBytes));
                            }

                         */
                            #endregion

                        }
                        Thread.Sleep(1000);
                    }

                    //Thread.Sleep(1000);
                }
            }
            catch(Exception error)
            {
                ServiceLog($"Exception: {error}");
            } 
        }

        static void Main(string[] args)
        {
            ServiceIsActive = true;
            Console.WriteLine("Сервис начал работу.");
            ServiceLog("Сервис начал работу.");

            //TCP сервер для прибора
            Thread TCPServerThread = new Thread(new ThreadStart(TCPServer));
            TCPServerThread.Name = "TCPServer";
            TCPServerThread.Start();

            // Обработка файлов с результатами
            //ResultsProcessing();

            // Поток обработки результатов
            Thread ResultProcessingThread = new Thread(ResultsProcessing);
            ResultProcessingThread.Name = "ResultsProcessing";
            ResultProcessingThread.Start();

            Console.ReadLine();
        }
    }
}
