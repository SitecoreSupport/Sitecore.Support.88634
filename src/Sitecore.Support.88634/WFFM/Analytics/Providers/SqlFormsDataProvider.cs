using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using Sitecore.Configuration;
using Sitecore.Diagnostics;
using Sitecore.WFFM.Abstractions.Analytics;
using Sitecore.WFFM.Abstractions.Data;
using Sitecore.WFFM.Abstractions.Shared;
using Sitecore.WFFM.Analytics.Model;

namespace Sitecore.Support.WFFM.Analytics.Providers
{
  public class SqlFormsDataProvider : IWffmDataProvider
  {
    private readonly IDbConnectionProvider connectionProvider;
    private readonly string connectionString;

    public SqlFormsDataProvider(string connectionStringName, ISettings settings,
      IDbConnectionProvider connectionProvider)
    {
      Assert.ArgumentNotNullOrEmpty(connectionStringName, "connectionStringName");
      Assert.ArgumentNotNull(settings, "settings");
      Assert.ArgumentNotNull(connectionProvider, "connectionProvider");
      connectionString = settings.GetConnectionString(connectionStringName);
      this.connectionProvider = connectionProvider;
    }

    public virtual IEnumerable<FormData> GetFormData(Guid formId)
    {
      if (Settings.GetSetting("WFM.IsRemoteActions", "false").Equals("true", StringComparison.InvariantCultureIgnoreCase)) return new List<FormData>();
      var list = new List<FormData>();
      var flag = false;
      using (var connection = connectionProvider.GetConnection(connectionString))
      {
        connection.Open();
        using (var command = connection.CreateCommand())
        {
          command.Connection = connection;
          command.CommandText =
            "SELECT [Id],[FormItemId],[ContactId],[InteractionId],[TimeStamp],[Data] FROM [FormData] WHERE [FormItemId]=@p1";
          command.Parameters.Add(new SqlParameter("p1", formId));
          command.CommandType = CommandType.Text;
          var reader = command.ExecuteReader();
          try
          {
            while (reader.Read())
            {
              var item = new FormData
              {
                Id = reader.GetGuid(0),
                FormID = reader.GetGuid(1),
                ContactId = reader.GetGuid(2),
                InteractionId = reader.GetGuid(3),
                Timestamp = reader.GetDateTime(4)
              };
              list.Add(item);
            }
          }
          catch
          {
            flag = true;
          }
          finally
          {
            reader.Close();
          }
        }
      }
      if (!flag && list.Count > 0)
        foreach (var data3 in list)
        {
          var list2 = new List<FieldData>();
          using (var connection2 = connectionProvider.GetConnection(connectionString))
          {
            connection2.Open();
            using (var command2 = connection2.CreateCommand())
            {
              command2.Connection = connection2;
              command2.CommandText =
                "SELECT [Id],[FieldItemId],[FieldName],[Value],[Data] FROM [FieldData] WHERE [FormId]=@p1";
              command2.Parameters.Add(new SqlParameter("p1", data3.Id));
              command2.CommandType = CommandType.Text;
              var reader2 = command2.ExecuteReader();
              try
              {
                while (reader2.Read())
                {
                  var data4 = new FieldData
                  {
                    Id = new Guid(reader2["Id"].ToString()),
                    FieldId = new Guid(reader2["FieldItemId"].ToString()),
                    FieldName = reader2["FieldName"] as string,
                    Form = data3,
                    Value = reader2["Value"] as string,
                    Data = reader2["Data"] as string
                  };
                  list2.Add(data4);
                }
              }
              catch
              {
                flag = true;
              }
              finally
              {
                reader2.Close();
              }
            }
          }
          if (list2.Count > 0)
            data3.Fields = list2;
        }
      return !flag ? list : new List<FormData>();
    }

    public virtual IEnumerable<IFormFieldStatistics> GetFormFieldsStatistics(Guid formId)
    {
      if (Settings.GetSetting("WFM.IsRemoteActions", "false").Equals("true", StringComparison.InvariantCultureIgnoreCase)) return new List<IFormFieldStatistics>();
      var list = new List<IFormFieldStatistics>();
      using (var connection = connectionProvider.GetConnection(connectionString))
      {
        connection.Open();
        using (var transaction = connection.BeginTransaction())
        {
          using (var command = connection.CreateCommand())
          {
            command.Transaction = transaction;
            command.Connection = connection;
            command.CommandText =
              "select FieldItemId as fieldid, max(FieldName) fieldname, COUNT(FormId) as submit_count \r\nfrom FieldData, FormData\r\nwhere FieldData.FormId=FormData.Id\r\nand FormItemId=@p1\r\ngroup by FieldItemId";
            command.Parameters.Add(new SqlParameter("p1", formId));
            command.CommandType = CommandType.Text;
            var reader = command.ExecuteReader();
            try
            {
              while (reader.Read())
              {
                var item = new FormFieldStatistics
                {
                  FieldId = new Guid(reader["fieldid"].ToString()),
                  FieldName = reader["fieldname"] as string,
                  Count = System.Convert.ToInt32(reader["submit_count"])
                };
                list.Add(item);
              }
            }
            finally
            {
              reader.Close();
            }
          }
          transaction.Commit();
        }
      }
      return list;
    }

    public virtual IEnumerable<IFormContactsResult> GetFormsStatisticsByContact(Guid formId, PageCriteria pageCriteria)
      =>
        new List<IFormContactsResult>();

    public virtual IFormStatistics GetFormStatistics(Guid formId)
    {
      int num;
      if (Settings.GetSetting("WFM.IsRemoteActions", "false").Equals("true", StringComparison.InvariantCultureIgnoreCase)) return new FormStatistics();
      using (var connection = connectionProvider.GetConnection(connectionString))
      {
        connection.Open();
        using (var transaction = connection.BeginTransaction())
        {
          using (var command = connection.CreateCommand())
          {
            command.Transaction = transaction;
            command.Connection = connection;
            command.CommandText = "SELECT COUNT(Id) AS submit_count FROM [FormData] WHERE [FormItemId]=@p1";
            command.Parameters.Add(new SqlParameter("p1", formId));
            command.CommandType = CommandType.Text;
            if (!int.TryParse((command.ExecuteScalar() ?? 0).ToString(), out num))
              num = 0;
          }
          transaction.Commit();
        }
      }
      return new FormStatistics
      {
        FormId = formId,
        SuccessSubmits = num
      };
    }

    public virtual void InsertFormData(FormData form)
    {
      if (Settings.GetSetting("WFM.IsRemoteActions", "false").Equals("true", StringComparison.InvariantCultureIgnoreCase)) return;
      var builder = new StringBuilder();
      using (var connection = connectionProvider.GetConnection(connectionString))
      {
        connection.Open();
        using (var transaction = connection.BeginTransaction())
        {
          using (var command = connection.CreateCommand())
          {
            var num = 1;
            command.Transaction = transaction;
            command.Connection = connection;
            var parameterValue = Guid.NewGuid();
            builder.AppendFormat(
              "INSERT INTO [FormData] ([Id],[FormItemId],[ContactId],[InteractionId],[Timestamp]) VALUES ( @{0}, @{1}, @{2}, @{3}, @{4} ) ",
              AddParameter(command.Parameters, num++, parameterValue),
              AddParameter(command.Parameters, num++, form.FormID),
              AddParameter(command.Parameters, num++, form.ContactId),
              AddParameter(command.Parameters, num++, form.InteractionId),
              AddParameter(command.Parameters, num++, form.Timestamp));
            if (form.Fields != null)
              foreach (var data in form.Fields)
              {
                var guid2 = Guid.NewGuid();
                object[] args =
                {
                  AddParameter(command.Parameters, num++, guid2),
                  AddParameter(command.Parameters, num++, parameterValue),
                  AddParameter(command.Parameters, num++, data.FieldId),
                  AddParameter(command.Parameters, num++, data.FieldName),
                  AddParameter(command.Parameters, num++, data.Value),
                  AddParameter(command.Parameters, num++, (object) data.Data ?? DBNull.Value)
                };
                builder.AppendFormat(
                  "INSERT INTO [FieldData] ([Id],[FormId],[FieldItemId],[FieldName],[Value],[Data]) VALUES ( @{0}, @{1}, @{2}, @{3}, @{4}, @{5} ) ",
                  args);
              }
            command.CommandText = builder.ToString();
            command.CommandType = CommandType.Text;
            command.ExecuteNonQuery();
          }
          transaction.Commit();
        }
      }
    }

    private string AddParameter(IDataParameterCollection parameters, int parameterNumber, object parameterValue)
    {
      var parameter = new SqlParameter("p" + parameterNumber, parameterValue);
      parameters.Add(parameter);
      return parameter.ParameterName;
    }
  }
}