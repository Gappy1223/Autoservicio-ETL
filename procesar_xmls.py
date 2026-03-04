import os
import xml.etree.ElementTree as ET
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)

NAMESPACES = {
    'cfdi': 'http://www.sat.gob.mx/cfdi/4',
    'cfdi33': 'http://www.sat.gob.mx/cfdi/3',
    'tfd': 'http://www.sat.gob.mx/TimbreFiscalDigital'
}

MAPEO_COMBUSTIBLE = {
    'MAGNA': 'Regular',
    'REGULAR': 'Regular',
    'GASOLINA MAGNA': 'Regular',
    'GASOLINA REGULAR': 'Regular',
    'G REGULAR': 'Regular',
    '87': 'Regular',
    'MAGNA SIN': 'Regular',
    'PREMIUM': 'Premium',
    '92': 'Premium',
    '93': 'Premium',
    'PREMIUM UBA': 'Premium',
    'DIESEL': 'Diesel',
    'GASOIL': 'Diesel',
    'COMBUSTIBLE DIESEL': 'Diesel'
}

def id_combustible(description):
    if not description:
        return None
    desc_upper = description.upper().strip()
    if desc_upper in MAPEO_COMBUSTIBLE:
        return MAPEO_COMBUSTIBLE(desc_upper)
    for key, value in MAPEO_COMBUSTIBLE.items():
        if key in desc_upper:
            return value
    print(f"Tipo de combustible no reconocido: {description}")
    return None

def calcular_turno(hora):
    if hora.hour >= 6 and hora.hour < 14:
        return 'Matutino'
    elif hora.hour >= 14 and hora.hour < 22:
        return 'Vespertino'
    else:
        return 'Nocturno'
    
def uuid_existente(uuid):
    query = text("select uuid from control_xml where uuid = :uuid")
    with engine.connect() as conn:
        result = conn.execute(query, {'uuid': uuid})
        return result.fetchone() is not None
    
def obtener_id_tipo_combustible(tipo):
    query = text("select id from dim_tipo_combustible where tipo = :tipo")
    with engine.connect() as conn:
        result = conn.execute(query, {'tipo': tipo})
        row = result.fetchone()
        return row[0] if row else None
    
def parse_xml_venta(xml_path, gasolinera_id = 1):
    nombre_archivo = os.path.basename(xml_path)
    print(f"Procesando: {nombre_archivo}")
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        version = root.get('Version')
        if version == '4.0':
            ns_cfdi = NAMESPACES('cfdi')
        else:
            ns_cfdi = NAMESPACES('cfdi33')

        uuid = None
        complemento = root.find('cfdi:Complemento', {'cfdi': ns_cfdi})
        if complemento is None:
            timbre = complemento.find('tfd:TimbreFiscalDigital', {'tdf': NAMESPACES['tfd']})
            if timbre is not None:
                uuid = timbre.get  ('UUID')

        if not uuid:
            print(f"UUID no encontrado {nombre_archivo}")
            return 'error'
        
        if uuid_existente(uuid):
            print(f"UUID duplicado {uuid}")
            return 'duplicado'
        
        fecha_str = root.get('Fecha')
        fecha_emision = datetime.fromisoformat(fecha_str.replace('T', ' '))

        emisor = root.find('cfdi:Emisor', {'cfdi': ns_cfdi})
        rfc_emisor = emisor.get('Rfc') if emisor is not None else None
        receptor = root.find('cfdi:Receptor', {'cfdi': ns_cfdi})
        rfc_receptor = receptor.get('Rfc') if receptor is not None else None
        nombre_receptor = receptor.get('Nombre') if receptor is not None else None

        conceptos = root.find('cfdi:Conceptos', {'cfdi': ns_cfdi})
        if conceptos is None:
            print(f"No se encontraron conceptos en {nombre_archivo}")
            return 'error'
        
        ventas_insertadas = 0
        for concepto in conceptos.findall('cfdi:Concepto', {'cfdi': ns_cfdi}):
            description = concepto.get('Descripcion', '').upper()
            cantidad = float(concepto.get('Cantidad', 0))
            valor_unitario = float(concepto.get('ValorUnitario', 0))
            importe = float(concepto.get('Importe', 0))

            tipo_combustible = id_combustible(description)
            if not tipo_combustible:
                print(f"Tipo de combustible no identificado: {description}")
                continue

            if cantidad <= 0:
                print(f"Cantidad invalida: {cantidad}")
                continue
            tipo_combustible_id = obtener_id_tipo_combustible(tipo_combustible)
            if not tipo_combustible_id:
                print(f"Tipo de combustible no encontrado en BD: {tipo_combustible}")
                continue

            turno = calcular_turno(fecha_emision)

            query = text("""
                insert into fact_ventas (
                    uuid, gasolinera_id, tipo_combustible_id, fecha_emision, fecha_operacion,
                    hora_operacion, turno, litros, precio_unitario, subtota, total, rfc_cliente, nombre_cliente
                    ) values (
                         :uuid, :gasolinera_id, :tipo_combustible_id,
                         :fecha_emision, :fecha_operacion, :hora_operacion, :turno,
                         :litros, :precio_unitario, :subtotal, :total,
                         :rfc_cliente, :nombre_cliente
                    )
            """)

            datos_venta = {
                'uuid': uuid,
                'gasolinera_id': gasolinera_id,
                'tipo_combustible_id': tipo_combustible_id,
                'fecha_emision': fecha_emision,
                'fecha_operacion': fecha_emision.date(),
                'hora_operacion': fecha_emision.time(),
                'turno': turno,
                'litros': cantidad,
                'precio_unitario': valor_unitario,
                'subtotal': importe,
                'total': importe,
                'rfc_cliente': rfc_receptor,
                'nombre_cliente': nombre_receptor
            }

            with engine.connect() as conn:
                conn.execute(query, datos_venta)
                conn.commit()
            ventas_insertadas += 1
            print(f"Venta insertada: {tipo_combustible} - {cantidad:.2f}L - ${importe:.2f}")

        query_control = text("""
            insert into control_xml (
                uuid, tipo, gasolinera_id, nombre_archivo, estatus, registros_generados
            ) values (
                :uuid, :tipo, :gasolinera_id, :nombre_archivo, :estatus, :registros)
        """)

        with engine.connect() as conn:
            conn.execute(query_control, {
                'uuid': uuid,
                'tipo': 'venta',
                'gasolinera_id': gasolinera_id,
                'nombre_archivo': nombre_archivo,
                'estatus': 'procesado',
                'registros': ventas_insertadas
            })
            conn.commit()
        
        print(f"{nombre_archivo} procesado exitosamente ({ventas_insertadas} ventas)")
        return 'procesado'
    
    except Exception as e:
        print(f"Error procesando: {nombre_archivo}: {e}")
        return 'error'
    
def parse_xml_compra(xml_path, gasolinera_id = 1):
    nombre_archivo = os.path.basename(xml_path)
    print(f"Procesando {nombre_archivo}")
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        version = root.get('Version')
        if version == '4.0':
            ns_cfdi = NAMESPACES['cfdi']
        else:
            ns_cfdi = NAMESPACES['cfdi33']

        uuid = None
        complemento = root.find('cfdi:Complemento', {'cfdi':ns_cfdi})
        if complemento is not None:
            timbre = complemento.find('tfd:TimbreFiscalDigital', {'tfd': NAMESPACES['tfd']})
            if timbre is not None:
                uuid = timbre.get('UUID')
        if not uuid:
            print(f"UUID no encontrado en: {nombre_archivo}")
            return 'error'
        
        if uuid_existente(uuid):
            print(f"UUID duplicado: {uuid}")
            return 'duplicado'
        
        fecha_str = root.get('Fecha')
        fecha_recepcion = datetime.fromisoformat(fecha_str.replace('T', ' '))
        emisor = root.find('cfdi:Emisor', {'cfdi': ns_cfdi})
        rfc_proveedor = emisor.get('Rfc') if emisor is not None else None
        nombre_proveedor = emisor.get('Nombre') if emisor is not None else None
        
        conceptos = root.find('cfdi:Conceptos', {'cfdi': ns_cfdi})
        if conceptos is None:
            print(f"No se encontraron conceptos en: {nombre_archivo}")
            return 'error'
        
        compras_insertadas = 0
        for concepto in conceptos.findall('cfdi:Concepto', {'cfdi': ns_cfdi}):
            description = concepto.get('Descripcion', '').upper()
            cantidad = float(concepto.get('Cantidad', 0))
            valor_unitario = float(concepto.get('ValorUnitario', 0))
            importe = float(concepto.get('Importe', 0))
            tipo_combustible = id_combustible(description)
            if not tipo_combustible:
                print(f"Concepto emitido (combustible no identificado): {description}")
                continue
            if cantidad <= 0:
                print(f"Cantidad invalida: {cantidad}")
                continue
            tipo_combustible_id = obtener_id_tipo_combustible(tipo_combustible)
            if not tipo_combustible_id:
                print(f"Tipo de combustible no encontrado en BD: {tipo_combustible}")
                continue

            query = text("""
                INSERT INTO fact_compras (
                    uuid, gasolinera_id, tipo_combustible_id,
                    fecha_recepcion, fecha_operacion,
                    litros, costo_unitario, subtotal, total,
                    rfc_proveedor, nombre_proveedor
                ) VALUES (
                    :uuid, :gasolinera_id, :tipo_combustible_id,
                    :fecha_recepcion, :fecha_operacion,
                    :litros, :costo_unitario, :subtotal, :total,
                    :rfc_proveedor, :nombre_proveedor
                )
            """)

            datos_compra = {
                'uuid': uuid,
                'gasolinera_id': gasolinera_id,
                'tipo_combustible_id': tipo_combustible_id,
                'fecha_recepcion': fecha_recepcion,
                'fecha_operacion': fecha_recepcion.date(),
                'litros': cantidad,
                'costo_unitario': valor_unitario,
                'subtotal': importe,
                'total': importe,
                'rfc_proveedor': rfc_proveedor,
                'nombre_proveedor': nombre_proveedor
            }

            with engine.connect() as conn:
                conn.execute(query, datos_compra)
                conn.commit()
            
            compras_insertadas +=1
            print(f"Compra insertada: {tipo_combustible} - {cantidad:.2f} - ${importe:.2f}")

        query_control = text("""
            INSERT INTO control_xml(
                uuid, tipo, gasolinera_id, nombre_archivo, estatus, registros_generados
                ) VALUES (
                    :uuid, :tipo, :gasolinera_id, :nombre_archivo, :estatus, :registros
                )
        """)
            
        with engine.connect() as conn:
            conn.execute(query_control, {
                'uuid': uuid,
                'tipo': 'compra',
                'gasolinera_id': gasolinera_id,
                'nombre_archivo': nombre_archivo,
                'estatus': 'procesado',
                'registros': compras_insertadas
            })
            conn.commit()
        
        print(f"{nombre_archivo} procesado correctamente ({compras_insertadas} compras)")
        return 'procesado'
    except Exception as e:
        print(f"Error procesando {nombre_archivo}: {e}")
        return 'error'


def mover_archivo(xml_path, destino):
    try:
        dest_dir = Path(destino)
        dest_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(xml_path), str(dest_dir / Path(xml_path).name))
    except Exception as e:
        print(f"Error moviendo archivo: {e}")

def procesar_carpeta(carpeta, tipo='venta', gasolinera_id = 1):
    carpeta_path = Path(carpeta)
    archivos_xml = list(carpeta_path.glob('*.xml'))
    if not archivos_xml:
        print(f"No se encontraron xmls en {carpeta}")
        return
    stats = {'procesados': 0, 'duplicados': 0, 'errores:': 0}
    for xml_file in archivos_xml:
        if tipo == 'venta':
            resultado = parse_xml_venta(xml_file, gasolinera_id)
        else:
            resultado = parse_xml_compra(xml_file, gasolinera_id)

        if resultado == 'procesado':
            stats['procesados'] += 1
            carpeta_procesados = carpeta_path / 'procesados'
            mover_archivo(xml_file, carpeta_procesados)
        elif resultado == 'duplicado':
            stats['duplicados'] += 1
            carpeta_procesados = carpeta / 'procesados'
            mover_archivo(xml_file, carpeta_procesados)
        elif resultado == 'error':
            stats['errores:'] += 1

if __name__ == "__main__":
    print("Procesar xmls")
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("Conexión a supabase existosa")
    except Exception as e:
        print(f"Error de conexión: {e}")
        exit(1)
    
    procesar_carpeta('datos/atlanta/ventas', tipo='venta', gasolinera_id=1)
    procesar_carpeta('datos/atlanta/compras', tipo='compra', gasolinera_id=1)
    



        




