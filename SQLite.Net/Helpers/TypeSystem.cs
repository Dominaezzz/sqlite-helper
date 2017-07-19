using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SQLite.Net.Helpers
{
	internal static class TypeSystem
	{
		internal static Type GetElementType(Type seqType)
		{
			return FindIEnumerable(seqType)?.GenericTypeArguments[0] ?? seqType;
		}

		private static Type FindIEnumerable(Type seqType)
		{
			if (seqType == null || seqType == typeof(string)) return null;
			if (seqType.IsArray) return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());

			if (seqType.GetTypeInfo().IsGenericType)
			{
				foreach (Type arg in seqType.GenericTypeArguments)
				{
					Type ienum = typeof(IEnumerable<>).MakeGenericType(arg);
					if (ienum.GetTypeInfo().IsAssignableFrom(seqType.GetTypeInfo()))
					{
						return ienum;
					}
				}

//				var temp = seqType.GetGenericArguments()
//					.Select(arg => typeof(IEnumerable<>).MakeGenericType(arg))
//					.FirstOrDefault(ienum => ienum.IsAssignableFrom(ienum));
			}

			Type[] ifaces = seqType.GetTypeInfo().ImplementedInterfaces.ToArray();
			if (ifaces != null && ifaces.Length > 0)
			{
				foreach (Type iface in ifaces)
				{
					Type ienum = FindIEnumerable(iface);

					if (ienum != null) return ienum;
				}
			}

			var baseType = seqType.GetTypeInfo().BaseType;
			if (baseType != null && baseType != typeof(object))
			{
				return FindIEnumerable(baseType);
			}
			return null;
		}
		
		internal static Type GetSequenceType(Type elementType)
		{
			return typeof(IEnumerable<>).MakeGenericType(elementType);
		}
		
		internal static bool IsNullableType(Type type)
		{
			return type != null && type.GetTypeInfo().IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
		}

		internal static bool IsNullAssignable(Type type)
		{
			return !type.GetTypeInfo().IsValueType || IsNullableType(type);
		}

		internal static Type GetNonNullableType(Type type)
		{
			return Nullable.GetUnderlyingType(type) ?? type;
		}

		internal static Type GetMemberType(MemberInfo mi)
		{
			return (mi as FieldInfo)?.FieldType ?? (mi as PropertyInfo)?.PropertyType ?? (mi as EventInfo)?.EventHandlerType;
		}
	}
}
