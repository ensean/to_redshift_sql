import os
from odps import ODPS


SCHEMA = 'ods'      # redshift 中的 schema名称

# 确保 ALIBABA_CLOUD_ACCESS_KEY_ID 环境变量设置为用户 Access Key ID，
# ALIBABA_CLOUD_ACCESS_KEY_SECRET 环境变量设置为用户 Access Key Secret，
odps_client = ODPS(
    os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID', ''),
    os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET', ''),
    project='store',    # MaxCompute 项目名称
    endpoint='http://service.cn-shenzhen.maxcompute.aliyun.com/api',
)
# https://www.alibabacloud.com/help/zh/maxcompute/user-guide/overview-32?spm=a2c63.p38356.0.0.989258f2nFIIYx

FIELD_MAP = {
    "BOOLEAN": "BOOLEAN",
    "TINYINT": "INT2",
    "SMALLINT": "INT4",
    "INT": "INT4",
    "BIGINT": "INT8",
    "FLOAT": "FLOAT4",
    "DOUBLE": "FLOAT8",
    "DECIMAL": "DECIMAL",
    "STRING": "VARCHAR(65535)",
    "VARCHAR": "VARCHAR",
    "BINARY": "VARBYTE",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "DATETIME": "VARCHAR(22)",
    "ARRAY": "VARCHAR(65535)",
    "MAP": "VARCHAR(65535)",
    "STRUCT": "VARCHAR(65535)"
}

def get_redshift_type(map_file,src_type):
    #  适配 VARCHAR、DECIMAL 参数
    if 'VARCHAR' in src_type.upper() or 'DECIMAL' in src_type.upper():
        return src_type
    else:
        return map_file.get(src_type.upper(), None)

def is_partition(col):
    # partion or field
    if 'partition' in col.__repr__():
        return True
    else:
        return False

def split_cols(mc_table):
    columns = mc_table.table_schema.columns
    fields = []
    partitions =[]
    for col in columns:
        if is_partition(col):
            partitions.append(col)
        else:
            fields.append(col)
    return {
        "fields": fields,
        "splited_cols": partitions
    }

def construct_redshift_create_sql(schema, table):
    """
    """
    table_name = table.name
    splited_cols = split_cols(table)
    PREFIX = "\nCREATE TABLE IF NOT EXISTS "
    final_sql = PREFIX + "%s.%s " % (schema, table_name)
    final_sql += "("
    for fld in splited_cols['fields']:
        field_def = "%s %s," % (fld.name, fld.type.name)
        field_def = field_def.replace(fld.type.name, get_redshift_type(FIELD_MAP, fld.type.name))
        final_sql += field_def

    #  将 MaxCompute 分区键添加为 Redshift 字段
    if len(splited_cols['splited_cols']) > 0:
        for pt in splited_cols['splited_cols']:
            final_sql += "%s %s," % (pt.name, get_redshift_type(FIELD_MAP, pt.type.name))
    final_sql = final_sql[:-1] + ")"

    # 将MaxCompute 分区键修改为 Redshift sortkey
    if len(splited_cols['splited_cols']) > 0:
        s_keys = []
        for pt in splited_cols['splited_cols']:
            s_keys.append(pt.name)
        final_sql += " sortkey (" + ",".join(s_keys) + ")"

    return final_sql


def main():
    with open('./table_create_sql.txt', "w") as f:
        for table in odps_client.list_tables():
            sql = construct_redshift_create_sql(SCHEMA, table)
            f.writelines(sql)

if __name__ == '__main__':
    main()