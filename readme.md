Desafío Data Engineer – Arquitectura Medallion con PySpark + Docker

1. Descripción del Problema y Contexto de Negocio

Una empresa del rubro retail necesita construir un pipeline de ingeniería de datos capaz de procesar información histórica de ventas, productos y clientes. 
El objetivo es transformar los datos desde su formato crudo (raw) hacia una estructura optimizada que permita análisis posteriores mediante features agregadas por cliente.

Para lograrlo, se requiere implementar una arquitectura de procesamiento por capas (Medallion Architecture) utilizando PySpark, ejecutándose en un ambiente local aislado en este caso se utilizo docker, para que soporte datasets grandes y permita:

    Trazabilidad entre capas

    Limpieza y estandarización

    Cálculo de indicadores de negocio

    Preparación final para modelos analíticos

2. Arquitectura Medallion Implementada
   
<img width="921" height="391" alt="image" src="https://github.com/user-attachments/assets/6aa69c82-7dca-4f63-9550-cc175a3f6d50" />



   La solución se compone de tres capas:

        Bronze – Ingesta cruda

            Lee archivos CSV desde /data/raw/

            Estandariza nombres de columnas

            Agrega metadata (ingestion_timestamp, source_file)

            Guarda en formato Parquet en /data/bronze/

        Silver – Limpieza y Enriquecimiento

            Elimina duplicados

            Aplica validaciones de calidad

            Estandariza tipos de datos

            Une órdenes header/detalle para generar fact_order_line

        Gold – Features y Agregaciones

            Filtra los últimos 90 días de datos

            Agrega ventas por cliente (monto, unidades, líneas)

            Genera gold_sales_customer_3m

            Genera tabla final dim_features

3. Estructura del Repositorio

        desafio_carozzi_docker/
        │
        ├── docker-compose.yml
        ├── Dockerfile
        ├── requirements.txt
        │
        ├── data/
        │   ├── raw/
        │   ├── bronze/
        │   ├── silver/
        │   └── gold/
        │
        └── src/
            ├── config.py
            ├── main.py
            ├── bronze.py
            ├── silver.py
            ├── gold.py
            └── check_data.py

4. Instalación y Ejecución
    Requisitos Previos:

        Docker Desktop instalado

        Git instalado

        Primero Clonar el repositorio:
   
        git clone https://github.com/Escanorelorgullo/desafio_carozzi_docker.git
        cd desafio_carozzi_docker

        Segundo Construir y ejecutar el pipeline completo
   
        docker compose up --build

        Si los pasos anteriores "NO FUNCIONAN" hacer lo siguiente:

        1.- descargar todos los archivos a tu pc local en una carpeta
        2.- abrir del archivo data_drive y descargar lo que esta en ese link (que es la carpeta data) en la carpeta creada
        3.- abrir CMD y posicionarse en la carpeta donde estan los archivos ejemplo: C:\Users\RonaldAguilera\Desktop\desafio_carozzi_docker
        4.- luego de tener instalado docker desktop ejecutar el comando: docker compose up --build el cual ejecutara todo lo necesario
        5.- Para ver las tablas creadas y algunas metricas ejecutar el sigueinte comando: docker compose run --rm etl python -m src.check_data


    El contenedor ejecutará automáticamente:

        Ingesta Bronze

        Transformaciones Silver

        Cálculos Gold

        Generación de dim_features

        Tercero para validar resultados

        Ejecutar el script de validaciones con el siguiente comando:

        docker compose run --rm etl python -m src.check_data

6. Resumen de Cada Capa

  


        Bronze
            Archivo	             Origen	           Descripción
            ----------------------------------------------------
            customers	       CSV crudo	    Datos de clientes
            products	       CSV crudo	    Datos de productos
            orders_header	   CSV crudo	    Encabezado de pedidos
            orders_detail	   CSV crudo	    Líneas de productos vendidos
    
        Silver
            Tabla	                               Descripción
            ----------------------------------------------------
            dim_customer	                    Dimensión limpia de clientes
            dim_product	                        Dimensión limpia de productos
            fact_order_line	                    Hechos de órdenes con join limpio y columnas no duplicadas
    
        Gold
            Tabla	                               Descripción
            ----------------------------------------------------
            gold_sales_customer_3m	            Ventas agregadas de los últimos 90 días por cliente
            dim_features	                    Tabla final requerida, con features por cliente

<img width="1137" height="817" alt="image" src="https://github.com/user-attachments/assets/0bf465ea-b0e9-4a5e-bde1-ad789f78dca9" />


7. Diccionario de Datos – Tabla dim_features


        | Columna                        | Tipo    | Descripción                                                                                                  |
        |--------------------------------|---------|--------------------------------------------------------------------------------------------------------------|
        | `customer_id`                  | int     | Identificador único del cliente.                                                                            |
        | `ventas_total_3m`              | double  | Suma del monto neto de ventas del cliente en los últimos 3 meses (consistente con `order_net_amount`).      |
        | `recencia_dias`                | int     | Número de días entre la fecha de referencia (última fecha de datos disponible) y la fecha del último pedido del cliente. |
        | `frecuencia_pedidos_3m`        | int     | Cantidad de pedidos realizados por el cliente en los últimos 3 meses.                                       |
        | `ticket_promedio_3m`           | double  | Promedio de venta por pedido en los últimos 3 meses: `ventas_total_3m / frecuencia_pedidos_3m` (solo clientes con ≥1 pedido). |
        | `dias_promedio_entre_pedidos`  | double  | Promedio de días entre pedidos consecutivos del cliente dentro de los últimos 3 meses (si tiene ≥2 pedidos).|
        | `variedad_categorias_3m`       | int     | Número de categorías distintas (`product_category`) compradas por el cliente en los últimos 3 meses.        |
        | `porcentaje_pedidos_promo_3m`  | double  | Porcentaje de pedidos del cliente que tuvieron al menos una línea con `promo_flag = 1` en los últimos 3 meses. |
        | `mix_credito_vs_contado_3m`    | double  | Porcentaje de ventas en crédito (`tipo_pago = 'credito'`) sobre el total de ventas del cliente en los últimos 3 meses. |


8. Migración a Arquitectura en Microsoft Fabric

<img width="921" height="377" alt="image" src="https://github.com/user-attachments/assets/032c1eee-0d05-41b8-97a2-13481d5ce702" />

<img width="921" height="517" alt="image" src="https://github.com/user-attachments/assets/90b28de6-ad67-4360-8f4e-3acaa65ae2c4" />



    Servicios recomendados:

        Capa	                                    Servicio recomendado
        ----------------------------------------------------------------------
        Ingesta	                                    Fabric Data Pipelines
        Transformación	                            Fabric Notebooks (Spark)
        Almacenamiento	                            Lakehouse (OneLake) con Delta Lake
        Orquestación	                            Fabric Orquestacion
        Seguridad	                                Purview, Onelake Access Control
        Gobierno	                                Microsoft Purview (Lineage, catálogo, políticas)

    Flujo sugerido en Fabric

        A. OneLake (Bronze)
            Ingesta directa vía pipelines --> almacenamiento Delta.
    
        B. Notebook Spark (Silver)
            Limpieza, cast de tipos, joins, calidad de datos.
    
        C. Notebook Spark (Gold)
            Cálculo de features, agregados y métricas.
    
        D. Warehouse / Lakehouse Gold
            Consumo por BI, modelos predictivos o Power BI.
    
        E. Purview
            Lineage entre lakehouse, pipelines, reporteria.

8. Tabla Final Entregada

        Generada automáticamente en:

        /data/gold/dim_features/

        En Formato Delta









