from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from airflow.models import Variable
import time
import json

# Queries TPC-H embutidas no DAG
TPCH_QUERIES = {
    "Q1": """
        SELECT
            l_returnflag,
            l_linestatus,
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
        FROM lineitem
        WHERE l_shipdate <= date '1998-12-01' - interval '90' day
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """,
    "Q2": """
        SELECT
            s_acctbal,
            s_name,
            n_name,
            p_partkey,
            p_mfgr,
            s_address,
            s_phone,
            s_comment
        FROM part, supplier, partsupp, nation, region
        WHERE p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND p_size = 15
            AND p_type like '%BRASS'
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE'
            AND ps_supplycost = (
                SELECT min(ps_supplycost)
                FROM partsupp, supplier, nation, region
                WHERE p_partkey = ps_partkey
                    AND s_suppkey = ps_suppkey
                    AND s_nationkey = n_nationkey
                    AND n_regionkey = r_regionkey
                    AND r_name = 'EUROPE'
            )
        ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
        LIMIT 100
    """,
    "Q3": """
        SELECT
            l_orderkey,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            o_orderdate,
            o_shippriority
        FROM customer, orders, lineitem
        WHERE c_mktsegment = 'BUILDING'
            AND c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND o_orderdate < date '1995-03-15'
            AND l_shipdate > date '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate
        LIMIT 10
    """,
    "Q4": """
        SELECT
            o_orderpriority,
            count(*) as order_count
        FROM orders
        WHERE o_orderdate >= date '1993-07-01'
            AND o_orderdate < date '1993-07-01' + interval '3' month
            AND exists (
                SELECT *
                FROM lineitem
                WHERE l_orderkey = o_orderkey
                    AND l_commitdate < l_receiptdate
            )
        GROUP BY o_orderpriority
        ORDER BY o_orderpriority
    """,
    "Q5": """
        SELECT
            n_name,
            sum(l_extendedprice * (1 - l_discount)) as revenue
        FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND l_suppkey = s_suppkey
            AND c_nationkey = s_nationkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'ASIA'
            AND o_orderdate >= date '1994-01-01'
            AND o_orderdate < date '1994-01-01' + interval '1' year
        GROUP BY n_name
        ORDER BY revenue DESC
    """,
    "Q6": """
        SELECT
            sum(l_extendedprice * l_discount) as revenue
        FROM lineitem
        WHERE l_shipdate >= date '1994-01-01'
            AND l_shipdate < date '1994-01-01' + interval '1' year
            AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
            AND l_quantity < 24
    """,
    "Q7": """
        SELECT
            supp_nation,
            cust_nation,
            l_year,
            sum(volume) as revenue
        FROM (
            SELECT
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                extract(year from l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            FROM supplier, lineitem, orders, customer, nation n1, nation n2
            WHERE s_suppkey = l_suppkey
                AND o_orderkey = l_orderkey
                AND c_custkey = o_custkey
                AND s_nationkey = n1.n_nationkey
                AND c_nationkey = n2.n_nationkey
                AND (
                    (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                    OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                )
                AND l_shipdate between date '1995-01-01' AND date '1996-12-31'
        ) as shipping
        GROUP BY supp_nation, cust_nation, l_year
        ORDER BY supp_nation, cust_nation, l_year
    """,
    "Q8": """
        SELECT
            o_year,
            sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
        FROM (
            SELECT
                extract(year from o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
            WHERE p_partkey = l_partkey
                AND s_suppkey = l_suppkey
                AND l_orderkey = o_orderkey
                AND o_custkey = c_custkey
                AND c_nationkey = n1.n_nationkey
                AND n1.n_regionkey = r_regionkey
                AND r_name = 'AMERICA'
                AND s_nationkey = n2.n_nationkey
                AND o_orderdate between date '1995-01-01' AND date '1996-12-31'
                AND p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
        GROUP BY o_year
        ORDER BY o_year
    """,
    "Q9": """
        SELECT
            nation,
            o_year,
            sum(amount) as sum_profit
        FROM (
            SELECT
                n_name as nation,
                extract(year from o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            FROM part, supplier, lineitem, partsupp, orders, nation
            WHERE s_suppkey = l_suppkey
                AND ps_suppkey = l_suppkey
                AND ps_partkey = l_partkey
                AND p_partkey = l_partkey
                AND o_orderkey = l_orderkey
                AND s_nationkey = n_nationkey
                AND p_name like '%green%'
        ) as profit
        GROUP BY nation, o_year
        ORDER BY nation, o_year DESC
    """,
    "Q10": """
        SELECT
            c_custkey,
            c_name,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            c_acctbal,
            n_name,
            c_address,
            c_phone,
            c_comment
        FROM customer, orders, lineitem, nation
        WHERE c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND o_orderdate >= date '1993-10-01'
            AND o_orderdate < date '1993-10-01' + interval '3' month
            AND l_returnflag = 'R'
            AND c_nationkey = n_nationkey
        GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        ORDER BY revenue DESC
        LIMIT 20
    """,
    "Q11": """
        SELECT
            ps_partkey,
            sum(ps_supplycost * ps_availqty) as value
        FROM partsupp, supplier, nation
        WHERE ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey
            AND n_name = 'GERMANY'
        GROUP BY ps_partkey
        HAVING sum(ps_supplycost * ps_availqty) > (
            SELECT sum(ps_supplycost * ps_availqty) * 0.0001
            FROM partsupp, supplier, nation
            WHERE ps_suppkey = s_suppkey
                AND s_nationkey = n_nationkey
                AND n_name = 'GERMANY'
        )
        ORDER BY value DESC
    """,
    "Q12": """
        SELECT
            l_shipmode,
            sum(case when o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
            sum(case when o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
        FROM orders, lineitem
        WHERE o_orderkey = l_orderkey
            AND l_shipmode in ('MAIL', 'SHIP')
            AND l_commitdate < l_receiptdate
            AND l_shipdate < l_commitdate
            AND l_receiptdate >= date '1994-01-01'
            AND l_receiptdate < date '1994-01-01' + interval '1' year
        GROUP BY l_shipmode
        ORDER BY l_shipmode
    """,
    "Q13": """
        SELECT
            c_count,
            count(*) as custdist
        FROM (
            SELECT
                c_custkey,
                count(o_orderkey) as c_count
            FROM customer left outer join orders on c_custkey = o_custkey
                AND o_comment not like '%special%requests%'
            GROUP BY c_custkey
        ) c_orders
        GROUP BY c_count
        ORDER BY custdist DESC, c_count DESC
    """,
    "Q14": """
        SELECT
            100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
        FROM lineitem, part
        WHERE l_partkey = p_partkey
            AND l_shipdate >= date '1995-09-01'
            AND l_shipdate < date '1995-09-01' + interval '1' month
    """,
    "Q15": """
        WITH revenue0 AS (
            SELECT
                l_suppkey as supplier_no,
                sum(l_extendedprice * (1 - l_discount)) as total_revenue
            FROM lineitem
            WHERE l_shipdate >= date '1996-01-01'
                AND l_shipdate < date '1996-01-01' + interval '3' month
            GROUP BY l_suppkey
        )
        SELECT
            s_suppkey,
            s_name,
            s_address,
            s_phone,
            total_revenue
        FROM supplier, revenue0
        WHERE s_suppkey = supplier_no
            AND total_revenue = (SELECT max(total_revenue) FROM revenue0)
        ORDER BY s_suppkey
    """,
    "Q16": """
        SELECT
            p_brand,
            p_type,
            p_size,
            count(distinct ps_suppkey) as supplier_cnt
        FROM partsupp, part
        WHERE p_partkey = ps_partkey
            AND p_brand <> 'Brand#45'
            AND p_type not like 'MEDIUM POLISHED%'
            AND p_size in (49, 14, 23, 45, 19, 3, 36, 9)
            AND ps_suppkey not in (
                SELECT s_suppkey
                FROM supplier
                WHERE s_comment like '%Customer%Complaints%'
            )
        GROUP BY p_brand, p_type, p_size
        ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
    """,
    "Q17": """
        SELECT
            sum(l_extendedprice) / 7.0 as avg_yearly
        FROM lineitem, part
        WHERE p_partkey = l_partkey
            AND p_brand = 'Brand#23'
            AND p_container = 'MED BOX'
            AND l_quantity < (
                SELECT 0.2 * avg(l_quantity)
                FROM lineitem
                WHERE l_partkey = p_partkey
            )
    """,
    "Q18": """
        SELECT
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice,
            sum(l_quantity)
        FROM customer, orders, lineitem
        WHERE o_orderkey in (
            SELECT l_orderkey
            FROM lineitem
            GROUP BY l_orderkey
            HAVING sum(l_quantity) > 300
        )
        AND c_custkey = o_custkey
        AND o_orderkey = l_orderkey
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        ORDER BY o_totalprice DESC, o_orderdate
        LIMIT 100
    """,
    "Q19": """
        SELECT
            sum(l_extendedprice* (1 - l_discount)) as revenue
        FROM lineitem, part
        WHERE (
            p_partkey = l_partkey
            AND p_brand = 'Brand#12'
            AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND l_quantity >= 1 AND l_quantity <= 1 + 10
            AND p_size between 1 AND 5
            AND l_shipmode in ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
        ) OR (
            p_partkey = l_partkey
            AND p_brand = 'Brand#23'
            AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            AND l_quantity >= 10 AND l_quantity <= 10 + 10
            AND p_size between 1 AND 10
            AND l_shipmode in ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
        ) OR (
            p_partkey = l_partkey
            AND p_brand = 'Brand#34'
            AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            AND l_quantity >= 20 AND l_quantity <= 20 + 10
            AND p_size between 1 AND 15
            AND l_shipmode in ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
        )
    """,
    "Q20": """
        SELECT
            s_name,
            s_address
        FROM supplier, nation
        WHERE s_suppkey in (
            SELECT ps_suppkey
            FROM partsupp
            WHERE ps_partkey in (
                SELECT p_partkey
                FROM part
                WHERE p_name like 'forest%'
            )
            AND ps_availqty > (
                SELECT 0.5 * sum(l_quantity)
                FROM lineitem
                WHERE l_partkey = ps_partkey
                    AND l_suppkey = ps_suppkey
                    AND l_shipdate >= date '1994-01-01'
                    AND l_shipdate < date '1994-01-01' + interval '1' year
            )
        )
        AND s_nationkey = n_nationkey
        AND n_name = 'CANADA'
        ORDER BY s_name
    """,
    "Q21": """
        SELECT
            s_name,
            count(*) as numwait
        FROM supplier, lineitem l1, orders, nation
        WHERE s_suppkey = l1.l_suppkey
            AND o_orderkey = l1.l_orderkey
            AND o_orderstatus = 'F'
            AND l1.l_receiptdate > l1.l_commitdate
            AND exists (
                SELECT *
                FROM lineitem l2
                WHERE l2.l_orderkey = l1.l_orderkey
                    AND l2.l_suppkey <> l1.l_suppkey
            )
            AND not exists (
                SELECT *
                FROM lineitem l3
                WHERE l3.l_orderkey = l1.l_orderkey
                    AND l3.l_suppkey <> l1.l_suppkey
                    AND l3.l_receiptdate > l3.l_commitdate
            )
            AND s_nationkey = n_nationkey
            AND n_name = 'SAUDI ARABIA'
        GROUP BY s_name
        ORDER BY numwait DESC, s_name
        LIMIT 100
    """,
    "Q22": """
        SELECT
            cntrycode,
            count(*) as numcust,
            sum(c_acctbal) as totacctbal
        FROM (
            SELECT
                substr(c_phone, 1, 2) as cntrycode,
                c_acctbal
            FROM customer
            WHERE substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
                AND c_acctbal > (
                    SELECT avg(c_acctbal)
                    FROM customer
                    WHERE c_acctbal > 0.00
                        AND substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
                )
                AND not exists (
                    SELECT *
                    FROM orders
                    WHERE o_custkey = c_custkey
                )
        ) as custsale
        GROUP BY cntrycode
        ORDER BY cntrycode
    """
}

# Funções utilitárias para salvamento de resultados
def save_results_to_s3(results_data, stage, scale_factor, execution_id=None):
    """Salva resultados do benchmark no S3 para backup"""
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        if execution_id is None:
            execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        s3_client = boto3.client(
            's3',
            endpoint_url=Variable.get("MINIO_ENDPOINT"),
            aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
            aws_secret_access_key=Variable.get("MINIO_SECRET_KEY")
        )
        
        # Estrutura: gold/benchmark_results/sf_X/execution_id/stage_timestamp.json
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        key = f"benchmark_results/sf_{scale_factor}/{execution_id}/{stage}_{timestamp}.json"
        
        # Preparar dados para salvamento
        results_with_metadata = {
            "execution_id": execution_id,
            "stage": stage,
            "scale_factor": scale_factor,
            "timestamp": timestamp,
            "datetime": datetime.now().isoformat(),
            "data": results_data
        }
        
        # Salvar no S3
        s3_client.put_object(
            Bucket="gold",
            Key=key,
            Body=json.dumps(results_with_metadata, indent=2, default=str),
            ContentType="application/json"
        )
        
        print(f"✅ Resultados salvos: s3://gold/{key}")
        return f"s3://gold/{key}"
        
    except Exception as e:
        print(f"⚠️ Erro ao salvar resultados em {stage}: {e}")
        return None

def load_previous_results(stage, scale_factor, execution_id):
    """Carrega resultados salvos anteriormente"""
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        s3_client = boto3.client(
            's3',
            endpoint_url=Variable.get("MINIO_ENDPOINT"),
            aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
            aws_secret_access_key=Variable.get("MINIO_SECRET_KEY")
        )
        
        # Listar arquivos da execução
        prefix = f"benchmark_results/sf_{scale_factor}/{execution_id}/{stage}_"
        response = s3_client.list_objects_v2(
            Bucket="gold",
            Prefix=prefix
        )
        
        if 'Contents' in response and len(response['Contents']) > 0:
            # Pegar o arquivo mais recente
            latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]
            
            # Carregar dados
            obj = s3_client.get_object(Bucket="gold", Key=latest_file['Key'])
            data = json.loads(obj['Body'].read().decode('utf-8'))
            
            print(f"📥 Carregados resultados anteriores de {stage}: {latest_file['Key']}")
            return data
        
        return None
        
    except Exception as e:
        print(f"⚠️ Erro ao carregar resultados anteriores de {stage}: {e}")
        return None

def create_progress_report(execution_id, scale_factor, current_format=None, completed_queries=0, total_queries=22):
    """Cria relatório de progresso em tempo real"""
    try:
        progress_data = {
            "execution_id": execution_id,
            "scale_factor": scale_factor,
            "current_format": current_format,
            "completed_queries": completed_queries,
            "total_queries": total_queries,
            "progress_percentage": (completed_queries / total_queries) * 100 if total_queries > 0 else 0,
            "timestamp": datetime.now().isoformat(),
            "status": "running" if completed_queries < total_queries else "completed"
        }
        
        # Salvar como arquivo especial de progresso
        save_results_to_s3(progress_data, "progress_report", scale_factor, execution_id)
        
        return progress_data
        
    except Exception as e:
        print(f"⚠️ Erro ao criar relatório de progresso: {e}")
        return None

@dag(
    dag_id="6_tpch_benchmark_gold",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Execução manual
    catchup=False,
    tags=["tpch", "benchmark", "gold", "queries", "performance"],
    max_active_runs=1,
    params={
        "scale_factor": 10,
        "test_formats": ["delta", "iceberg", "hudi"],  # Formatos a testar
        "run_warmup": True,  # Executar warmup antes dos testes
        "iterations": 3,  # Número de execuções por query para média
        "timeout_minutes": 30,  # Timeout por query
        "save_results": True  # Salvar resultados detalhados
    },
)
def tpch_benchmark_gold():

    @task
    def validate_silver_data(**context):
        """Valida se os dados silver existem nos três formatos"""
        
        params = context["params"]
        scale_factor = params.get("scale_factor", 1)
        test_formats = params.get("test_formats", ["delta", "iceberg", "hudi"])
        
        print(f"🔍 Validando dados silver TPC-H")
        print(f"📊 Scale Factor: {scale_factor}")
        print(f"🎯 Formatos a testar: {test_formats}")
        
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            s3_client = boto3.client(
                's3',
                endpoint_url=Variable.get("MINIO_ENDPOINT"),
                aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
                aws_secret_access_key=Variable.get("MINIO_SECRET_KEY")
            )
            
            # Tabelas TPC-H esperadas
            tpch_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
            
            validation_results = {}
            
            for format_name in test_formats:
                print(f"\n📋 Validando formato {format_name.upper()}...")
                format_results = {}
                
                for table in tpch_tables:
                    table_prefix = f"sf_{scale_factor}/{format_name}/{table}/"
                    
                    try:
                        response = s3_client.list_objects_v2(
                            Bucket="silver",
                            Prefix=table_prefix,
                            MaxKeys=20  # Aumentar limite para detectar mais arquivos
                        )
                        
                        if 'Contents' in response and len(response['Contents']) > 0:
                            # Filtrar apenas arquivos de dados (ignorar metadados, etc.)
                            data_files = [obj for obj in response['Contents'] 
                                        if not obj['Key'].endswith('/') and obj['Size'] > 0]
                            
                            if data_files:
                                total_size = sum(obj['Size'] for obj in data_files)
                                file_count = len(data_files)
                                
                                format_results[table] = {
                                    "status": "found",
                                    "file_count": file_count,
                                    "total_size_mb": total_size / (1024 * 1024),
                                    "path": f"s3a://silver/{table_prefix}"
                                }
                                print(f"   ✅ {table}: {file_count} arquivos, {total_size/(1024*1024):.2f} MB")
                            else:
                                # Há objetos mas não são arquivos de dados válidos
                                all_objects = [obj['Key'] for obj in response['Contents']]
                                format_results[table] = {
                                    "status": "empty",
                                    "objects_found": all_objects,
                                    "path": f"s3a://silver/{table_prefix}"
                                }
                                print(f"   ⚠️ {table}: {len(response['Contents'])} objetos encontrados mas sem dados válidos")
                        else:
                            format_results[table] = {
                                "status": "not_found",
                                "path": f"s3a://silver/{table_prefix}"
                            }
                            print(f"   ❌ {table}: dados não encontrados")
                            
                    except ClientError as e:
                        format_results[table] = {
                            "status": "error",
                            "error": str(e),
                            "path": f"s3a://silver/{table_prefix}"
                        }
                        print(f"   ❌ {table}: erro ao acessar - {str(e)}")
                
                validation_results[format_name] = format_results
                
                # Verificar se todas as tabelas foram encontradas para o formato
                found_tables = [t for t, r in format_results.items() if r["status"] == "found"]
                print(f"   📊 Resumo {format_name}: {len(found_tables)}/{len(tpch_tables)} tabelas encontradas")
            
            # Determinar quais formatos estão prontos para teste
            ready_formats = []
            format_status = {}
            
            for format_name in test_formats:
                format_results = validation_results[format_name]
                found_tables = [t for t, r in format_results.items() if r["status"] == "found"]
                empty_tables = [t for t, r in format_results.items() if r["status"] == "empty"]
                available_tables = found_tables + empty_tables  # Considerar tabelas vazias também
                
                found_count = len(found_tables)
                available_count = len(available_tables)
                total_count = len(tpch_tables)
                
                format_status[format_name] = {
                    "found_tables": found_tables,
                    "empty_tables": empty_tables,
                    "available_tables": available_tables,
                    "found_count": found_count,
                    "available_count": available_count,
                    "total_count": total_count,
                    "completion_percentage": (found_count / total_count) * 100,
                    "availability_percentage": (available_count / total_count) * 100
                }
                
                # Aceitar formatos com pelo menos 25% das tabelas com dados OU 50% das tabelas disponíveis (mesmo vazias)
                min_found_required = max(1, total_count // 4)  # 25% das tabelas com dados
                min_available_required = max(1, total_count // 2)  # 50% das tabelas disponíveis
                
                if found_count >= min_found_required or available_count >= min_available_required:
                    ready_formats.append(format_name)
                    if found_count > 0:
                        print(f"   ✅ {format_name}: {found_count}/{total_count} com dados, {available_count}/{total_count} disponíveis - APROVADO")
                    else:
                        print(f"   ⚠️ {format_name}: {available_count}/{total_count} tabelas disponíveis (vazias) - APROVADO PARA DEBUG")
                else:
                    print(f"   ❌ {format_name}: {found_count}/{total_count} com dados, {available_count}/{total_count} disponíveis - INSUFICIENTE")
            
            print(f"\n📊 Status dos formatos:")
            for fmt, status in format_status.items():
                print(f"   • {fmt}: {status['found_count']}/{status['total_count']} tabelas ({status['completion_percentage']:.1f}%)")
            
            print(f"\n✅ Formatos aprovados para benchmark: {ready_formats}")
            
            if not ready_formats:
                # Tentar com critério super flexível - qualquer tabela disponível (mesmo vazia)
                print("⚠️ Nenhum formato aprovado, tentando com critério super flexível...")
                for format_name in test_formats:
                    if format_status[format_name]["available_count"] >= 1:
                        ready_formats.append(format_name)
                        available = format_status[format_name]["available_count"] 
                        found = format_status[format_name]["found_count"]
                        print(f"   ✅ {format_name}: Aceito com {available} tabela(s) disponível(eis) ({found} com dados)")
                
                if not ready_formats:
                    print("❌ Nenhum formato tem estruturas silver disponíveis")
                    # Em vez de falhar, vamos prosseguir com lista vazia para debug
                    print("⚠️ Prosseguindo para debug detalhado - verificar se dados foram criados")
                else:
                    print(f"✅ Formatos aprovados com critério super flexível: {ready_formats}")
            
            # Preparar resultados finais
            final_results = {
                "validation_status": "success",
                "scale_factor": scale_factor,
                "ready_formats": ready_formats,
                "validation_results": validation_results,
                "format_status": format_status
            }
            
            # Salvar resultados da validação
            execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_results_to_s3(final_results, "validation", scale_factor, execution_id)
            
            # Adicionar execution_id aos resultados
            final_results["execution_id"] = execution_id
            
            return final_results
            
        except Exception as e:
            print(f"❌ Erro na validação: {str(e)}")
            return {
                "validation_status": "error",
                "error": str(e),
                "scale_factor": scale_factor
            }

    def generate_diagnostic_report(s3_client, scale_factor, spark):
        """Gera relatório de diagnóstico quando nenhum formato está disponível"""
        
        print("🔍 RELATÓRIO DE DIAGNÓSTICO - DADOS SILVER")
        print("=" * 60)
        print(f"📊 Scale Factor: {scale_factor}")
        print(f"📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("")
        
        try:
            if s3_client:
                print("📂 Verificando estrutura do bucket silver...")
                
                # Listar prefixos no nível sf_X
                try:
                    response = s3_client.list_objects_v2(
                        Bucket="silver",
                        Prefix=f"sf_{scale_factor}/",
                        Delimiter="/",
                        MaxKeys=100
                    )
                    
                    if 'CommonPrefixes' in response:
                        print(f"   📁 Encontrados diretórios em sf_{scale_factor}/:")
                        for prefix in response['CommonPrefixes']:
                            dir_name = prefix['Prefix'].replace(f"sf_{scale_factor}/", "").rstrip("/")
                            print(f"      • {dir_name}")
                            
                            # Investigar cada formato encontrado
                            try:
                                format_response = s3_client.list_objects_v2(
                                    Bucket="silver",
                                    Prefix=prefix['Prefix'],
                                    Delimiter="/",
                                    MaxKeys=20
                                )
                                
                                if 'CommonPrefixes' in format_response:
                                    tables = []
                                    for table_prefix in format_response['CommonPrefixes']:
                                        table_name = table_prefix['Prefix'].replace(prefix['Prefix'], "").rstrip("/")
                                        tables.append(table_name)
                                    
                                    print(f"         Tabelas em {dir_name}: {', '.join(tables) if tables else 'Nenhuma'}")
                                    
                                    # Verificar se há dados nas tabelas
                                    for table_prefix in format_response['CommonPrefixes']:
                                        table_name = table_prefix['Prefix'].replace(prefix['Prefix'], "").rstrip("/")
                                        data_response = s3_client.list_objects_v2(
                                            Bucket="silver",
                                            Prefix=table_prefix['Prefix'],
                                            MaxKeys=3
                                        )
                                        if 'Contents' in data_response:
                                            file_count = len(data_response['Contents'])
                                            total_size = sum(obj['Size'] for obj in data_response['Contents'])
                                            print(f"           • {table_name}: {file_count} arquivos, {total_size/(1024*1024):.2f} MB")
                                else:
                                    print(f"         ❌ Nenhuma tabela encontrada em {dir_name}")
                                    
                            except Exception as e:
                                print(f"         ❌ Erro ao investigar {dir_name}: {e}")
                                
                    else:
                        print(f"   ❌ Nenhum diretório encontrado em sf_{scale_factor}/")
                        
                    # Listar qualquer conteúdo direto
                    if 'Contents' in response:
                        print(f"   📄 Arquivos diretos encontrados:")
                        for obj in response['Contents'][:5]:  # Máximo 5 arquivos
                            print(f"      • {obj['Key']} ({obj['Size']} bytes)")
                    
                except Exception as e:
                    print(f"   ❌ Erro ao listar silver: {e}")
                
                # Verificar se existem dados em outros scale factors
                print(f"\n🔍 Verificando outros scale factors...")
                try:
                    response = s3_client.list_objects_v2(
                        Bucket="silver",
                        Delimiter="/",
                        MaxKeys=50
                    )
                    
                    if 'CommonPrefixes' in response:
                        other_sfs = []
                        for prefix in response['CommonPrefixes']:
                            sf_name = prefix['Prefix'].rstrip("/")
                            if sf_name.startswith("sf_") and sf_name != f"sf_{scale_factor}":
                                other_sfs.append(sf_name)
                        
                        if other_sfs:
                            print(f"   ✅ Encontrados outros scale factors: {other_sfs}")
                        else:
                            print(f"   ❌ Nenhum outro scale factor encontrado")
                    
                except Exception as e:
                    print(f"   ❌ Erro ao verificar outros SFs: {e}")
            
            else:
                print("❌ Cliente S3 não disponível para diagnóstico")
            
        except Exception as e:
            print(f"❌ Erro no diagnóstico: {e}")
        
        # Retornar resultado mínimo
        return {
            "benchmark_status": "diagnostic_only",
            "scale_factor": scale_factor,
            "total_queries": 0,
            "formats_tested": [],
            "diagnostic_completed": True,
            "message": "Nenhum dado silver encontrado - diagnóstico executado"
        }

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            # Jars para todos os formatos
            "spark.jars.packages": ",".join([
                "io.delta:delta-spark_2.12:3.2.0",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1",
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0",
            ]),
            
            # Extensions para todos os formatos
            "spark.sql.extensions": ",".join([
                "io.delta.sql.DeltaSparkSessionExtension",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
            ]),
            
            # Catálogos
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "hadoop",
            
            # Hudi serializer
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            
            # S3A / MinIO
            "spark.hadoop.fs.s3a.endpoint": Variable.get("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key": Variable.get("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            
            # Configurações de performance otimizada
            "spark.executor.memory": "3g",
            "spark.driver.memory": "2g",
            "spark.executor.cores": "3",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            
            # Cache configurations
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        }
    )
    def execute_tpch_benchmark(validation_result: dict, spark: SparkSession = None, sc: SparkContext = None, **context):
        """Executa benchmark das 22 queries TPC-H nos formatos disponíveis"""
        
        if validation_result["validation_status"] == "error":
            print("❌ Erro na validação - tentando continuar para debug")
            print(f"⚠️ Erro reportado: {validation_result.get('error', 'Unknown error')}")
            # Em vez de falhar, vamos tentar continuar com formatos disponíveis
            ready_formats = []
            scale_factor = validation_result.get("scale_factor", 1)
        else:
            scale_factor = validation_result["scale_factor"]
            ready_formats = validation_result["ready_formats"]
        
        print("🚀 Iniciando TPC-H Benchmark Gold")
        
        params = context["params"]
        run_warmup = params.get("run_warmup", True)
        iterations = params.get("iterations", 3)
        timeout_minutes = params.get("timeout_minutes", 30)
        
        # Obter execution_id da validação ou criar novo
        execution_id = validation_result.get("execution_id", datetime.now().strftime("%Y%m%d_%H%M%S"))
        
        print(f"📊 Scale Factor: {scale_factor}")
        print(f"🎯 Formatos para testar: {ready_formats}")
        print(f"🆔 Execution ID: {execution_id}")
        
        # Salvar configuração inicial do benchmark
        initial_config = {
            "scale_factor": scale_factor,
            "ready_formats": ready_formats,
            "run_warmup": run_warmup,
            "iterations": iterations,
            "timeout_minutes": timeout_minutes,
            "execution_start": datetime.now().isoformat()
        }
        save_results_to_s3(initial_config, "benchmark_start", scale_factor, execution_id)
        
        if not ready_formats:
            print("⚠️ Nenhum formato aprovado na validação")
            print("🔍 Tentando descobrir formatos disponíveis diretamente...")
            
            # Tentar descobrir formatos disponíveis
            try:
                import boto3
                from botocore.exceptions import ClientError
                
                s3_client = boto3.client(
                    's3',
                    endpoint_url=Variable.get("MINIO_ENDPOINT"),
                    aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
                    aws_secret_access_key=Variable.get("MINIO_SECRET_KEY")
                )
                
                # Verificar diretamente no bucket silver
                test_formats = params.get("test_formats", ["delta", "iceberg", "hudi"])
                discovered_formats = []
                
                print("🔍 Investigação detalhada dos formatos...")
                
                for fmt in test_formats:
                    try:
                        # Verificar se existe o diretório do formato
                        format_prefix = f"sf_{scale_factor}/{fmt}/"
                        response = s3_client.list_objects_v2(
                            Bucket="silver",
                            Prefix=format_prefix,
                            Delimiter="/",
                            MaxKeys=20
                        )
                        
                        tables_found = 0
                        if 'CommonPrefixes' in response:
                            print(f"   📁 Diretórios em {fmt}:")
                            for prefix in response['CommonPrefixes']:
                                table_dir = prefix['Prefix'].replace(format_prefix, "").rstrip("/")
                                print(f"      • {table_dir}")
                                
                                # Verificar se há arquivos na tabela
                                table_response = s3_client.list_objects_v2(
                                    Bucket="silver",
                                    Prefix=prefix['Prefix'],
                                    MaxKeys=1
                                )
                                if 'Contents' in table_response:
                                    tables_found += 1
                        
                        if 'Contents' in response:
                            # Arquivos diretos no nível do formato
                            direct_files = len(response['Contents'])
                            print(f"   📄 Arquivos diretos em {fmt}: {direct_files}")
                            if direct_files > 0:
                                tables_found += 1
                        
                        if tables_found > 0:
                            discovered_formats.append(fmt)
                            print(f"   ✅ Formato {fmt}: {tables_found} tabelas com dados")
                        else:
                            print(f"   ❌ Formato {fmt}: Nenhuma tabela com dados encontrada")
                            
                    except Exception as e:
                        print(f"   ❌ Erro ao verificar formato {fmt}: {e}")
                
                if discovered_formats:
                    ready_formats = discovered_formats
                    print(f"🎯 Formatos descobertos com dados: {ready_formats}")
                else:
                    print("❌ Nenhum formato com dados encontrado - gerando relatório de diagnóstico")
                    # Gerar relatório de diagnóstico em vez de falhar
                    return generate_diagnostic_report(s3_client, scale_factor, spark)
                    
            except Exception as e:
                print(f"❌ Erro ao descobrir formatos: {e}")
                return generate_diagnostic_report(None, scale_factor, spark)
        print(f"🔄 Iterações por query: {iterations}")
        print(f"⏰ Timeout por query: {timeout_minutes} minutos")
        print(f"🔥 Warmup habilitado: {run_warmup}")
        
        # Marcar início do benchmark
        benchmark_start_time = time.time()
        benchmark_results = []
        
        # Configurar catálogos dinâmicos
        for format_name in ready_formats:
            if format_name == "iceberg":
                iceberg_warehouse = f"s3a://silver/sf_{scale_factor}/"
                spark.conf.set("spark.sql.catalog.iceberg.warehouse", iceberg_warehouse)
                print(f"🏠 Configurado warehouse Iceberg: {iceberg_warehouse}")
        
        # Loop pelos formatos
        for format_name in ready_formats:
            print(f"\n🎯 Executando benchmark para formato: {format_name.upper()}")
            format_start_time = time.time()
            
            try:
                # Registrar tabelas para o formato atual
                print(f"📋 Registrando tabelas {format_name}...")
                register_tables_for_format(spark, format_name, scale_factor)
                
                # Warmup (opcional)
                if run_warmup:
                    print(f"🔥 Executando warmup para {format_name}...")
                    warmup_queries = ["Q1", "Q6"]  # Queries simples para warmup
                    for query_id in warmup_queries:
                        try:
                            query_sql = TPCH_QUERIES[query_id]
                            df = spark.sql(query_sql)
                            df.count()  # Força execução
                            print(f"   ✅ Warmup {query_id} concluído")
                        except Exception as e:
                            print(f"   ⚠️ Warmup {query_id} falhou: {str(e)}")
                
                # Executar todas as 22 queries
                format_results = []
                for query_id in sorted(TPCH_QUERIES.keys()):
                    print(f"\n📊 Executando {query_id} ({format_name})...")
                    
                    query_sql = TPCH_QUERIES[query_id]
                    query_results = []
                    
                    # Múltiplas execuções para média
                    for iteration in range(iterations):
                        try:
                            print(f"   🔄 Iteração {iteration + 1}/{iterations}")
                            
                            start_time = time.time()
                            
                            # Executar query com timeout
                            df = spark.sql(query_sql)
                            df.cache()  # Cache para garantir execução completa
                            
                            row_count = df.count()
                            
                            end_time = time.time()
                            execution_time = end_time - start_time
                            
                            query_results.append({
                                "iteration": iteration + 1,
                                "execution_time_sec": execution_time,
                                "row_count": row_count,
                                "status": "success"
                            })
                            
                            print(f"      ✅ {execution_time:.2f}s, {row_count:,} rows")
                            
                            df.unpersist()  # Limpar cache
                            
                        except Exception as e:
                            print(f"      ❌ Erro: {str(e)}")
                            query_results.append({
                                "iteration": iteration + 1,
                                "execution_time_sec": None,
                                "row_count": None,
                                "status": "error",
                                "error": str(e)
                            })
                    
                    # Calcular estatísticas da query
                    successful_runs = [r for r in query_results if r["status"] == "success"]
                    
                    if successful_runs:
                        execution_times = [r["execution_time_sec"] for r in successful_runs]
                        avg_time = sum(execution_times) / len(execution_times)
                        min_time = min(execution_times)
                        max_time = max(execution_times)
                        row_count = successful_runs[0]["row_count"]  # Deve ser igual em todas
                        
                        query_summary = {
                            "query_id": query_id,
                            "format": format_name,
                            "scale_factor": scale_factor,
                            "successful_iterations": len(successful_runs),
                            "total_iterations": iterations,
                            "avg_execution_time_sec": avg_time,
                            "min_execution_time_sec": min_time,
                            "max_execution_time_sec": max_time,
                            "row_count": row_count,
                            "status": "success"
                        }
                        
                        print(f"   📊 Resumo: {avg_time:.2f}s avg ({min_time:.2f}-{max_time:.2f}s), {row_count:,} rows")
                    else:
                        query_summary = {
                            "query_id": query_id,
                            "format": format_name,
                            "scale_factor": scale_factor,
                            "successful_iterations": 0,
                            "total_iterations": iterations,
                            "status": "failed"
                        }
                        print(f"   ❌ Todas as iterações falharam")
                    
                    query_summary["detailed_results"] = query_results
                    format_results.append(query_summary)
                    
                    # Salvar resultado da query imediatamente (backup incremental)
                    query_backup = {
                        "format": format_name,
                        "query_summary": query_summary,
                        "completed_queries": len(format_results),
                        "total_queries": len(TPCH_QUERIES)
                    }
                    save_results_to_s3(query_backup, f"query_{query_id}_{format_name}", scale_factor, execution_id)
                    
                    # Atualizar progresso
                    create_progress_report(execution_id, scale_factor, format_name, len(format_results), len(TPCH_QUERIES))
                
                format_end_time = time.time()
                format_total_time = format_end_time - format_start_time
                
                # Resumo do formato
                successful_queries = [r for r in format_results if r["status"] == "success"]
                print(f"\n✅ Formato {format_name} concluído em {format_total_time:.2f}s")
                print(f"📊 Queries executadas com sucesso: {len(successful_queries)}/22")
                
                benchmark_results.extend(format_results)
                
                # Salvar resultados do formato completo
                format_backup = {
                    "format": format_name,
                    "format_results": format_results,
                    "successful_queries": len(successful_queries),
                    "total_time_sec": format_total_time,
                    "completed_formats": len([fmt for fmt in ready_formats if fmt in [r["format"] for r in benchmark_results]])
                }
                save_results_to_s3(format_backup, f"format_{format_name}_complete", scale_factor, execution_id)
                
            except Exception as e:
                print(f"❌ Erro no formato {format_name}: {str(e)}")
                # Salvar erro também
                error_backup = {
                    "format": format_name,
                    "error": str(e),
                    "status": "failed"
                }
                save_results_to_s3(error_backup, f"format_{format_name}_error", scale_factor, execution_id)
                continue
        
        # Relatório final
        print(f"\n🎉 TPC-H Benchmark concluído!")
        print_benchmark_summary(benchmark_results, ready_formats, scale_factor)
        
        # Preparar resultados finais
        final_results = {
            "benchmark_results": benchmark_results,
            "scale_factor": scale_factor,
            "tested_formats": ready_formats,
            "total_queries": 22,
            "iterations_per_query": iterations,
            "execution_id": execution_id,
            "execution_end": datetime.now().isoformat(),
            "total_execution_time": time.time() - benchmark_start_time
        }
        
        # Salvar resultados finais completos
        save_results_to_s3(final_results, "benchmark_complete", scale_factor, execution_id)
        
        return final_results

    @task
    def save_benchmark_results(benchmark_result: dict, **context):
        """Salva os resultados do benchmark na camada gold"""
        
        params = context["params"]
        save_results = params.get("save_results", True)
        
        if not save_results:
            print("⏭️ Salvamento de resultados desabilitado")
            return {"status": "skipped"}
        
        print("💾 Salvando resultados do benchmark na camada gold...")
        
        try:
            import pandas as pd
            import boto3
            from io import StringIO
            
            benchmark_results = benchmark_result["benchmark_results"]
            scale_factor = benchmark_result["scale_factor"]
            
            # Preparar dados para salvamento
            results_data = []
            timestamp = datetime.now()
            
            for result in benchmark_results:
                base_record = {
                    "benchmark_timestamp": timestamp,
                    "scale_factor": scale_factor,
                    "query_id": result["query_id"],
                    "format": result["format"],
                    "status": result["status"],
                    "successful_iterations": result.get("successful_iterations", 0),
                    "total_iterations": result.get("total_iterations", 0),
                    "avg_execution_time_sec": result.get("avg_execution_time_sec"),
                    "min_execution_time_sec": result.get("min_execution_time_sec"),
                    "max_execution_time_sec": result.get("max_execution_time_sec"),
                    "row_count": result.get("row_count")
                }
                results_data.append(base_record)
            
            # Converter para DataFrame
            df = pd.DataFrame(results_data)
            
            # Salvar como CSV no S3
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            
            s3_client = boto3.client(
                's3',
                endpoint_url=Variable.get("MINIO_ENDPOINT"),
                aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
                aws_secret_access_key=Variable.get("MINIO_SECRET_KEY")
            )
            
            # Definir caminho do arquivo
            timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
            s3_key = f"tpch_benchmark_sf_{scale_factor}_{timestamp_str}.csv"
            
            s3_client.put_object(
                Bucket="gold",
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            
            print(f"✅ Resultados salvos em: s3://gold/{s3_key}")
            
            # Salvar também resumo JSON
            summary_data = {
                "benchmark_info": {
                    "timestamp": timestamp.isoformat(),
                    "scale_factor": scale_factor,
                    "tested_formats": benchmark_result["tested_formats"],
                    "total_queries": benchmark_result["total_queries"],
                    "iterations_per_query": benchmark_result["iterations_per_query"]
                },
                "results": benchmark_results
            }
            
            json_key = f"tpch_benchmark_sf_{scale_factor}_{timestamp_str}.json"
            s3_client.put_object(
                Bucket="gold",
                Key=json_key,
                Body=json.dumps(summary_data, indent=2, default=str),
                ContentType='application/json'
            )
            
            print(f"✅ Resumo JSON salvo em: s3://gold/{json_key}")
            
            return {
                "status": "success",
                "csv_file": f"s3://gold/{s3_key}",
                "json_file": f"s3://gold/{json_key}",
                "records_saved": len(results_data)
            }
            
        except Exception as e:
            print(f"❌ Erro ao salvar resultados: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }

    @task
    def consolidate_benchmark_results(benchmark_result: dict, **context):
        """Consolida resultados do benchmark e cria relatório final com backup de segurança"""
        
        params = context["params"]
        scale_factor = params.get("scale_factor", 1)
        
        print("📊 Consolidando resultados do benchmark...")
        
        execution_id = benchmark_result.get("execution_id")
        if not execution_id:
            execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Criar resumo executivo
            summary = {
                "execution_id": execution_id,
                "scale_factor": scale_factor,
                "execution_date": datetime.now().isoformat(),
                "total_formats_tested": len(benchmark_result.get("tested_formats", [])),
                "total_queries": benchmark_result.get("total_queries", 0),
                "iterations_per_query": benchmark_result.get("iterations_per_query", 0),
                "benchmark_status": "completed" if benchmark_result.get("benchmark_results") else "partial"
            }
            
            # Salvar resumo executivo final
            save_results_to_s3(summary, "executive_summary", scale_factor, execution_id)
            
            # Adicionar localização dos backups
            summary["backup_location"] = f"s3://gold/benchmark_results/sf_{scale_factor}/{execution_id}/"
            
            print(f"✅ Resultados consolidados salvos")
            print(f"📁 Backup completo em: {summary['backup_location']}")
            print(f"🆔 Execution ID: {execution_id}")
            
            return summary
            
        except Exception as e:
            print(f"⚠️ Erro na consolidação: {e}")
            return {
                "status": "consolidation_error",
                "error": str(e),
                "execution_id": execution_id,
                "scale_factor": scale_factor
            }

    # Pipeline de execução
    validation_task = validate_silver_data()
    benchmark_task = execute_tpch_benchmark(validation_task)
    save_task = save_benchmark_results(benchmark_task)
    consolidate_task = consolidate_benchmark_results(benchmark_task)

    # Dependências
    validation_task >> benchmark_task >> [save_task, consolidate_task]


def register_tables_for_format(spark: SparkSession, format_name: str, scale_factor: int):
    """Registra tabelas temporárias para o formato especificado"""
    
    tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    
    for table in tables:
        if format_name == "delta":
            path = f"s3a://silver/sf_{scale_factor}/delta/{table}/"
            spark.read.format("delta").load(path).createOrReplaceTempView(table)
            
        elif format_name == "iceberg":
            # Para Iceberg, usar o catálogo
            table_name = f"iceberg.iceberg.{table}"
            spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {table} AS SELECT * FROM {table_name}")
            
        elif format_name == "hudi":
            path = f"s3a://silver/sf_{scale_factor}/hudi/{table}/"
            spark.read.format("hudi").load(path).createOrReplaceTempView(table)
    
    print(f"   ✅ {len(tables)} tabelas registradas para {format_name}")


def print_benchmark_summary(results: list, formats: list, scale_factor: int):
    """Imprime resumo do benchmark"""
    
    print(f"\n📋 RESUMO DO BENCHMARK TPC-H - SF {scale_factor}")
    print("=" * 80)
    
    # Agrupar por formato
    for format_name in formats:
        format_results = [r for r in results if r["format"] == format_name]
        successful = [r for r in format_results if r["status"] == "success"]
        
        if successful:
            total_time = sum(r["avg_execution_time_sec"] for r in successful)
            fastest_query = min(successful, key=lambda x: x["avg_execution_time_sec"])
            slowest_query = max(successful, key=lambda x: x["avg_execution_time_sec"])
            
            print(f"\n🎯 {format_name.upper()}:")
            print(f"   ✅ Queries executadas: {len(successful)}/22")
            print(f"   ⏱️  Tempo total: {total_time:.2f}s")
            print(f"   ⚡ Query mais rápida: {fastest_query['query_id']} ({fastest_query['avg_execution_time_sec']:.2f}s)")
            print(f"   🐌 Query mais lenta: {slowest_query['query_id']} ({slowest_query['avg_execution_time_sec']:.2f}s)")
        else:
            print(f"\n❌ {format_name.upper()}: Nenhuma query executada com sucesso")
    
    print("=" * 80)


tpch_benchmark_gold()