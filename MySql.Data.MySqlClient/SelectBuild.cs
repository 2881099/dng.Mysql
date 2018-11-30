using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MySql.Data.MySqlClient {
	public partial interface IDAL {
		string Table { get; }
		string Field { get; }
		string Sort { get; }
		object GetItem(IDataReader dr, ref int index);
		Task<(object result, int dataIndex)> GetItemAsync(MySqlDataReader dr, int index);
	}
	public class SelectBuild<TReturnInfo, TLinket> : SelectBuild<TReturnInfo> where TLinket : SelectBuild<TReturnInfo> {
		protected TLinket Where1Or(string filterFormat, Array values) {
			if (values == null) values = new object[] { null };
			if (values.Length == 0) return this as TLinket;
			if (values.Length == 1) return base.Where(filterFormat, values.GetValue(0)) as TLinket;
			string filter = string.Empty;
			for (int a = 0; a < values.Length; a++) filter = string.Concat(filter, " OR ", string.Format(filterFormat, "{" + a + "}"));
			object[] parms = new object[values.Length];
			values.CopyTo(parms, 0);
			return base.Where(filter.Substring(4), parms) as TLinket;
		}
		/// <summary>
		/// 若使用读写分离，默认查询【从库】，使用本方法明确查询【主库】
		/// </summary>
		/// <returns></returns>
		public new TLinket Master() => base.Master() as TLinket;
		public new TLinket Count(out long count) => base.Count(out count) as TLinket;
		public new TLinket Where(Expression<Func<TReturnInfo, bool>> expression) => base.Where(expression) as TLinket;
		public new TLinket Where(string filter, params object[] parms) => base.Where(true, filter, parms) as TLinket;
		public new TLinket Where(bool isadd, string filter, params object[] parms) => base.Where(isadd, filter, parms) as TLinket;
		public TLinket WhereExists<T>(SelectBuild<T> select, bool isNotExists = false) => this.Where((isNotExists ? "NOT " : "") + $"EXISTS({select.ToString("1")})") as TLinket;
		public new TLinket GroupBy(string groupby) => base.GroupBy(groupby) as TLinket;
		public new TLinket Having(string filter, params object[] parms) => base.Having(true, filter, parms) as TLinket;
		public new TLinket Having(bool isadd, string filter, params object[] parms) => base.Having(isadd, filter, parms) as TLinket;
		public new TLinket Sort(string sort) => base.Sort(sort) as TLinket;
		public new TLinket OrderBy(string sort) => base.Sort(sort) as TLinket;
		public new TLinket From<TBLL>() => base.From<TBLL>() as TLinket;
		public new TLinket From<TBLL>(string alias) => base.From<TBLL>(alias) as TLinket;
		public new TLinket As(string alias) => base.As(alias) as TLinket;
		public new TLinket InnerJoin(Expression<Func<TReturnInfo, bool>> on) => base.InnerJoin(on) as TLinket;
		public new TLinket LeftJoin(Expression<Func<TReturnInfo, bool>> on) => base.LeftJoin(on) as TLinket;
		public new TLinket RightJoin(Expression<Func<TReturnInfo, bool>> on) => base.RightJoin(on) as TLinket;
		public new TLinket InnerJoin<TBLL>(string alias, string on) => base.InnerJoin<TBLL>(alias, on) as TLinket;
		public new TLinket LeftJoin<TBLL>(string alias, string on) => base.LeftJoin<TBLL>(alias, on) as TLinket;
		public new TLinket RightJoin<TBLL>(string alias, string on) => base.RightJoin<TBLL>(alias, on) as TLinket;
		public new TLinket Skip(int skip) => base.Skip(skip) as TLinket;
		public new TLinket Limit(int limit) => base.Limit(limit) as TLinket;
		public new TLinket Take(int limit) => base.Limit(limit) as TLinket;
		public new TLinket Page(int pageIndex, int pageSize) => base.Page(pageIndex, pageSize) as TLinket;
		public SelectBuild(IDAL dal, Executer exec) : base(dal, exec) { }
	}
	public partial class SelectBuild<TReturnInfo> {
		protected int _limit, _skip;
		protected string _select = "SELECT ", _orderby, _field, _table, _join, _where, _groupby, _having;
		protected List<IDAL> _dals = new List<IDAL>();
		protected List<string> _dalsAlias = new List<string>();
		protected Executer _exec;
		public List<TReturnInfo> ToList(int expireSeconds, string cacheKey = null) {
			string sql = null;
			string[] objNames = new string[_dals.Count - 1];
			for (int b = 1; b < _dals.Count; b++) {
				string name = _dals[b].GetType().Name;
				objNames[b - 1] = string.Concat("Obj_", name[0].ToString().ToLower(), name.Substring(1));
			}
			if (expireSeconds > 0 && string.IsNullOrEmpty(cacheKey)) {
				sql = this.ToString();
				cacheKey = sql.Substring(sql.IndexOf(" \r\nFROM ") + 8);
			}
			List<object> cacheList = expireSeconds > 0 ? new List<object>() : null;
			return _exec.CacheShell(cacheKey, expireSeconds, () => {
				List<TReturnInfo> ret = new List<TReturnInfo>();
				if (string.IsNullOrEmpty(sql)) sql = this.ToString();
				_exec.ExecuteReader(dr => {
					int index = -1;
					TReturnInfo info = (TReturnInfo) _dals[0].GetItem(dr, ref index);
					Type type = info.GetType();
					ret.Add(info);
					if (cacheList != null) cacheList.Add(type.GetMethod("Stringify").Invoke(info, null));
					for (int b = 0; b < objNames.Length; b++) {
						object obj = _dals[b + 1].GetItem(dr, ref index);
						PropertyInfo prop = type.GetProperty(objNames[b]);
						if (prop == null) throw new Exception(string.Concat(type.FullName, " 没有定义属性 ", objNames[b]));
						if (obj != null) prop.SetValue(info, obj, null);
						if (cacheList != null) cacheList.Add(obj?.GetType().GetMethod("Stringify").Invoke(obj, null));
					}
				}, CommandType.Text, sql);
				return ret;
			}, list => JsonConvert.SerializeObject(cacheList), cacheValue => ToListDeserialize(cacheValue, objNames));
		}
		private List<TReturnInfo> ToListDeserialize(string cacheValue, string[] objNames) {
			List<TReturnInfo> ret = new List<TReturnInfo>();
			MethodInfo[] parses = new MethodInfo[_dals.Count];
			for (int b = 0; b < _dals.Count; b++) {
				string modelTypeName = string.Concat(_dals[b].GetType().FullName.Replace(".DAL.", ".Model."), "Info");
				parses[b] = this.GetType().GetTypeInfo().Assembly.GetType(modelTypeName).GetMethod("Parse", new Type[] { typeof(string) });
			}
			string[] vs = JsonConvert.DeserializeObject<string[]>(cacheValue);
			for (int a = 0, skip = objNames.Length + 1; a < vs.Length; a += skip) {
				TReturnInfo info = (TReturnInfo) parses[0].Invoke(null, new object[] { vs[a] });
				if (info == null) continue;
				Type type = info.GetType();
				for (int b = 1; b < parses.Length; b++) {
					object item = parses[b].Invoke(null, new object[] { vs[a + b] });
					if (item == null) continue;
					PropertyInfo prop = type.GetProperty(objNames[b - 1]);
					if (prop != null) prop.SetValue(info, item, null);
				}
				ret.Add(info);
			}
			return ret;
		}
		public List<TReturnInfo> ToList() {
			return this.ToList(0);
		}
		public TReturnInfo ToOne() {
			List<TReturnInfo> ret = this.Limit(1).ToList();
			return ret.Count > 0 ? ret[0] : default(TReturnInfo);
		}
		public override string ToString() => this.ToString(null);
		public string ToString(string field) {
			if (string.IsNullOrEmpty(_orderby) && _skip > 0) this.Sort(_dals[0].Sort);
			string limit = _skip > 0 || _limit > 0 ? string.Format(" \r\nlimit {0},{1}", Math.Max(0, _skip), _limit > 0 ? _limit : -1) : string.Empty;
			string where = string.IsNullOrEmpty(_where) ? string.Empty : string.Concat(" \r\nWHERE ", _where.Substring(5));
			string sql = string.Concat(_select, field ?? _field, _table, _join, where, _orderby, limit);
			return sql;
		}
		/// <summary>
		/// 查询指定字段，返回元组或单值
		/// </summary>
		/// <typeparam name="T">元组或单值，如：.Aggregate&lt;(int id, string name)&gt;("id,title")，或 .Aggregate&lt;int&gt;("id")</typeparam>
		/// <param name="fields">返回的字段，用逗号分隔，如：id,name</param>
		/// <returns></returns>
		public List<T> Aggregate<T>(string fields) {
			string limit = _skip > 0 || _limit > 0 ? string.Format(" \r\nlimit {0},{1}", Math.Max(0, _skip), _limit > 0 ? _limit : -1) : string.Empty;
			string where = string.IsNullOrEmpty(_where) ? string.Empty : string.Concat(" \r\nWHERE ", _where.Substring(5));
			string having = string.IsNullOrEmpty(_groupby) ||
							string.IsNullOrEmpty(_having) ? string.Empty : string.Concat(" \r\nHAVING ", _having.Substring(5));
			string sql = string.Concat(_select, fields, _table, _join, where, _groupby, having, _orderby, limit);

			List<T> ret = new List<T>();
			Type type = typeof(T);

			_exec.ExecuteReader(dr => {
				int dataIndex = -1;
				var read = this.AggregateReadTuple(type, dr, ref dataIndex);
				ret.Add(read == null ? default(T) : (T)read);
			}, CommandType.Text, sql);
			return ret;
		}
		public T AggregateScalar<T>(string field) {
			var items = this.Aggregate<T>(field);
			return items.Count > 0 ? items[0] : default(T);
		}
		protected object AggregateReadTuple(Type type, IDataReader dr, ref int dataIndex) {
			bool isTuple = type.Namespace == "System" && type.Name.StartsWith("ValueTuple`");
			if (isTuple) {
				FieldInfo[] fs = type.GetFields();
				Type[] types = new Type[fs.Length];
				object[] parms = new object[fs.Length];
				for (int a = 0; a < fs.Length; a++) {
					types[a] = fs[a].FieldType;
					parms[a] = this.AggregateReadTuple(types[a], dr, ref dataIndex);
				}
				ConstructorInfo constructor = type.GetConstructor(types);
				return constructor.Invoke(parms);
			}
			return dr.IsDBNull(++dataIndex) ? null : dr.GetValue(dataIndex);
		}
		/// <summary>
		/// 执行SQL，若查询语句存在记录则返回 true，否则返回 false
		/// </summary>
		/// <returns></returns>
		public bool Any() => this.AggregateScalar<long>("1") == 1;
		protected SelectBuild<TReturnInfo> Master() {
			_select = " SELECT "; // ExecuteReader 内会判断 StartsWith("SELECT ")，才使用从库查询
			return this;
		}
		public long Count() {
			return this.AggregateScalar<long>("count(1)");
		}
		protected SelectBuild<TReturnInfo> Count(out long count) {
			count = this.Count();
			return this;
		}
		public static SelectBuild<TReturnInfo> From(IDAL dal, Executer exec) {
			return new SelectBuild<TReturnInfo>(dal, exec);
		}
		int _fields_count = 0;
		string _mainAlias = "a";
		protected SelectBuild(IDAL dal, Executer exec) {
			_dals.Add(dal);
			_dalsAlias.Add(_mainAlias);
			_field = dal.Field;
			_table = string.Concat(" \r\nFROM ", dal.Table, " ", _mainAlias);
			_exec = exec;
		}
		protected SelectBuild<TReturnInfo> From<TBLL>() {
			return this.From<TBLL>(string.Empty);
		}
		protected SelectBuild<TReturnInfo> From<TBLL>(string alias) {
			IDAL dal = this.ConvertTBLL(typeof(TBLL));
			_table = string.Concat(_table, ", ", dal.Table, " ", alias);
			return this;
		}
		protected SelectBuild<TReturnInfo> As(string alias) {
			string table = string.Concat(" \r\nFROM ", _dals.FirstOrDefault()?.Table, " ", _mainAlias);
			if (_table.StartsWith(table)) {
				var fields = _field.Split(new[] { ", \r\n" }, 2, StringSplitOptions.None);
				fields[0] = fields[0].Replace(string.Concat(_mainAlias, ".`"), string.Concat(alias, ".`"));
				_field = string.Join(", \r\n", fields);
				_table = string.Concat(string.IsNullOrEmpty(_mainAlias) ? table : table.Remove(table.Length - _mainAlias.Length), _mainAlias = alias, _table.Substring(table.Length));
				_dalsAlias[0] = alias;
			}
			return this;
		}
		protected IDAL ConvertTBLL(Type bll) {
			string dalTypeName = bll.FullName.Replace(".BLL.", ".DAL.");
			IDAL dal = this.GetType().GetTypeInfo().Assembly.CreateInstance(dalTypeName) as IDAL;
			if (dal == null) throw new Exception(string.Concat("找不到类型 ", dalTypeName));
			return dal;
		}
		protected SelectBuild<TReturnInfo> Join(IDAL dal, string alias, string on, string joinType) {
			_dals.Add(dal);
			_dalsAlias.Add(alias);
			string fields2 = dal.Field.Replace("a.", string.Concat(alias, "."));
			string[] names = fields2.Split(new string[] { ", " }, StringSplitOptions.None);
			for (int a = 0; a < names.Length; a++) {
				string ast = string.Concat(" as", ++_fields_count);
				names[a] = string.Concat(names[a], ast);
			}
			_field = string.Concat(_field, ", \r\n", string.Join(", ", names));
			_join = string.Concat(_join, " \r\n", joinType, " ", dal.Table, " ", alias, " ON ", on);
			return this;
		}
		protected SelectBuild<TReturnInfo> Where(Expression<Func<TReturnInfo, bool>> expression) {
			var exp = this.ParseExpression(expression);
			if (exp.dal != null) {
				_table = string.Concat(_table, ", ", exp.dal.Table, " ", exp.alias);
			}
			return this.Where(true, exp.exp);
		}
		protected SelectBuild<TReturnInfo> Where(string filter, params object[] parms) {
			return this.Where(true, filter, parms);
		}
		protected SelectBuild<TReturnInfo> Where(bool isadd, string filter, params object[] parms) {
			if (isadd) {
				if (_mainAlias != "a") filter = string.Concat(filter).Replace("a.`", string.Concat(_mainAlias, ".`"));
				//将参数 = null 转换成 IS NULL
				if (parms != null && parms.Length > 0) {
					for (int a = 0; a < parms.Length; a++)
						if (parms[a] == null)
							filter = Regex.Replace(filter, @"\s+=\s+\{" + a + @"\}", " IS {" + a + "}");
				}
				_where = string.Concat(_where, " AND (", Executer.Addslashes(filter, parms), ")");
			}
			return this;
		}
		protected SelectBuild<TReturnInfo> GroupBy(string groupby) {
			_groupby = groupby;
			if (string.IsNullOrEmpty(_groupby)) return this;
			_groupby = string.Concat(" \r\nGROUP BY ", _groupby);
			return this;
		}
		protected SelectBuild<TReturnInfo> Having(string filter, params object[] parms) {
			return this.Having(true, filter, parms);
		}
		protected SelectBuild<TReturnInfo> Having(bool isadd, string filter, params object[] parms) {
			if (string.IsNullOrEmpty(_groupby)) return this;
			if (isadd) _having = string.Concat(_having, " AND (", Executer.Addslashes(filter, parms), ")");
			return this;
		}
		protected SelectBuild<TReturnInfo> Sort(string sort) {
			if (!string.IsNullOrEmpty(sort)) _orderby = string.Concat(" \r\nORDER BY ", sort);
			return this;
		}
		protected SelectBuild<TReturnInfo> OrderBy(string sort) {
			return this.Sort(sort);
		}
		public SelectBuild<TReturnInfo> InnerJoin(Expression<Func<TReturnInfo, bool>> on) {
			var exp = this.ParseExpression(on);
			if (exp.dal != null) this.Join(exp.dal, exp.alias, exp.exp, "INNER JOIN");
			return this.Where(exp.exp);
		}
		public SelectBuild<TReturnInfo> LeftJoin(Expression<Func<TReturnInfo, bool>> on) {
			var exp = this.ParseExpression(on);
			if (exp.dal != null) return this.Join(exp.dal, exp.alias, exp.exp, "LEFT JOIN");
			return this.Where(exp.exp);
		}
		public SelectBuild<TReturnInfo> RightJoin(Expression<Func<TReturnInfo, bool>> on) {
			var exp = this.ParseExpression(on);
			if (exp.dal != null) return this.Join(exp.dal, exp.alias, exp.exp, "RIGHT JOIN");
			return this.Where(exp.exp);
		}
		protected SelectBuild<TReturnInfo> InnerJoin<TBLL>(string alias, string on) {
			return this.Join(this.ConvertTBLL(typeof(TBLL)), alias, on, "INNER JOIN");
		}
		protected SelectBuild<TReturnInfo> LeftJoin<TBLL>(string alias, string on) {
			return this.Join(this.ConvertTBLL(typeof(TBLL)), alias, on, "LEFT JOIN");
		}
		protected SelectBuild<TReturnInfo> RightJoin<TBLL>(string alias, string on) {
			return this.Join(this.ConvertTBLL(typeof(TBLL)), alias, on, "RIGHT JOIN");
		}
		protected SelectBuild<TReturnInfo> Skip(int skip) {
			_skip = skip;
			return this;
		}
		protected SelectBuild<TReturnInfo> Limit(int limit) {
			_limit = limit;
			return this;
		}
		protected SelectBuild<TReturnInfo> Take(int limit) {
			return this.Limit(limit);
		}
		protected SelectBuild<TReturnInfo> Page(int pageIndex, int pageSize) {
			return this.Skip(Math.Max(0, pageIndex - 1) * pageSize).Limit(pageSize);
		}

		#region 表达式树解析
		private (IDAL dal, string alias, string exp) ParseExpression(Expression exp) {
			if (exp is LambdaExpression) return ParseExpression((exp as LambdaExpression).Body);
			if (exp is BinaryExpression) {
				var expBinary = exp as BinaryExpression;
				var left = ParseExpression(expBinary.Left);
				string oper = GetExpressionOperatorString(expBinary);
				var right = ParseExpression(expBinary.Right);
				if (right.exp == "NULL") oper = oper == "=" ? " IS " : " IS NOT ";
				var dal = left.dal ?? right.dal;
				string alias = null;
				if (left.dal != null) alias = left.alias;
				if (right.dal != null) alias = right.alias;
				return (dal, alias, left.exp + oper + right.exp);
			}
			if (exp is MemberExpression) return ParseMemberExpression(exp as MemberExpression);
			if (exp is ConstantExpression) return (null, null, Executer.Addslashes("{0}", (exp as ConstantExpression)?.Value));
			if (exp is UnaryExpression) return ParseExpression((exp as UnaryExpression).Operand);
			return (null, null, null);
		}
		private (IDAL dal, string alias, string exp) ParseMemberExpression(MemberExpression exp) {
			if (exp == null) return (null, null, null);
			if (exp.Member.Name == "Now" && exp.Type == typeof(DateTime)) return (null, null, "now()");
			else if (exp.NodeType == ExpressionType.MemberAccess && exp.Expression?.NodeType == ExpressionType.Parameter) return (null, null, $"{_mainAlias}.{exp.Member.Name}"); //主表字段
			else if (exp.NodeType == ExpressionType.MemberAccess) {
				MemberExpression mp2 = exp;
				while (true) {
					if (mp2.NodeType != ExpressionType.MemberAccess) throw new ArgumentException($"不能解析表达式 {exp}");

					if (mp2.Expression.Type.FullName.EndsWith("Info") == true && mp2.Expression.Type.FullName.IndexOf(".Model.") != -1) {
						var bll = mp2.Expression.Type.Assembly.GetType((string)mp2.Expression.Type.FullName.Remove((int)(mp2.Expression.Type.FullName.Length - 4)).Replace(".Model.", ".BLL."));
						var dal = ConvertTBLL((Type)bll);
						if (dal == null) break;

						var fkpath = "";
						var fkalias = "";
						var mp3 = mp2.Expression as MemberExpression;

						while (mp3 != null) {
							fkpath = mp3.Member.Name + "." + fkpath;

							var mp33 = mp3.Expression as MemberExpression;
							if (mp33 == null) break;
							mp3 = mp33;
						}
						fkpath = fkpath.Trim('.');
						fkalias = $"`{fkpath}`";

						if (mp3 != null && mp3.NodeType == ExpressionType.MemberAccess && mp3.Expression?.NodeType == ExpressionType.Parameter) {
							var find = _dals.Where((b, c) => b.Table == dal.Table && _dalsAlias[c] == fkalias);
							if (find.Any()) return ((IDAL)null, (string)null, $"{fkalias}.{mp2.Member.Name}");
							else return (dal, fkalias, $"{fkalias}.{mp2.Member.Name}");
						} else throw new ArgumentException($"不能解析表达式 {exp}");
					}

					var mp22 = mp2.Expression as MemberExpression;
					if (mp22 == null) break;
					mp2 = mp22;
				}
			}
			return (null, null, exp.Member.Name);
		}
		private string GetExpressionOperatorString(BinaryExpression exp) {
			switch (exp.NodeType) {
				case ExpressionType.OrElse: return " OR ";
				case ExpressionType.Or: return "|";
				case ExpressionType.AndAlso: return " AND ";
				case ExpressionType.And: return "&";
				case ExpressionType.GreaterThan: return ">";
				case ExpressionType.GreaterThanOrEqual: return ">=";
				case ExpressionType.LessThan: return "<";
				case ExpressionType.LessThanOrEqual: return "<=";
				case ExpressionType.NotEqual: return "<>";
				case ExpressionType.Add: return "+";
				case ExpressionType.Subtract: return "-";
				case ExpressionType.Multiply: return "*";
				case ExpressionType.Divide: return "/";
				case ExpressionType.Modulo: return "%";
				case ExpressionType.Equal: return "=";
			}
			return "";
		}
		#endregion
	}
}