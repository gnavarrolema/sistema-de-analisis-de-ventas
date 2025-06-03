from datetime import time
from decimal import Decimal, InvalidOperation

class Producto:
    """
    Representa un producto en el sistema de análisis de ventas.
    Incluye validaciones mejoradas para integridad de datos.
    """

    def __init__(self, id_producto: int, nombre_producto: str, precio: Decimal | float | str, id_categoria: int,
                 clase_producto: str | None = None, 
                 hora_modificacion: time | None = None, # CAMBIO: Se espera time|None, no string
                 resistente: str | None = None, 
                 es_alergenico: bool | None = None, # CAMBIO: Se espera bool|None, no string
                 dias_vitalidad: int | None = None):
        """
        Constructor de la clase Producto.

        Args:
            id_producto (int): El ID único del producto.
            nombre_producto (str): El nombre del producto (se espera ya procesado/limpio).
            precio (Decimal | float | str): El precio del producto (el setter se encarga de la conversión/validación).
            id_categoria (int): El ID de la categoría (se espera ya validado).
            clase_producto (str, optional): Clase (se espera ya validado).
            hora_modificacion (time, optional): Hora de modificación (se espera objeto time o None).
            resistente (str, optional): Resistencia (se espera ya validado).
            es_alergenico (bool, optional): Indicador alergénico (se espera bool o None).
            dias_vitalidad (int, optional): Días de vitalidad (el setter se encarga de la conversión/validación).
        
        NOTA: Para los atributos asignados directamente a `self._atributo` en este constructor,
        las validaciones y transformaciones de sus respectivos setters no se aplican en la inicialización.
        Los datos deben ser proporcionados ya en el formato y tipo correcto.
        """
        self._id_producto = id_producto
        self._nombre_producto = nombre_producto
        self.precio = precio  # Utiliza el setter para validación y conversión
        self._id_categoria = id_categoria
        self._clase_producto = clase_producto
        self._hora_modificacion = hora_modificacion
        self._resistente = resistente
        self._es_alergenico = es_alergenico
        self.dias_vitalidad = dias_vitalidad # Utiliza el setter para validación y conversión


    @staticmethod
    def _parse_modify_date_str(time_str: str) -> time:
        """
        Convierte una cadena de tiempo en formato "MM:SS.f" o "MM:SS" a un objeto datetime.time.
        Este método es útil si se necesita procesar el string ANTES de pasarlo al constructor.
        """
        try:
            parts = time_str.split(':')
            if len(parts) != 2:
                raise ValueError("Formato debe ser MM:SS.f o MM:SS")

            minutes = int(parts[0])
            
            seconds_parts = parts[1].split('.')
            seconds = int(seconds_parts[0])
            
            microseconds = 0
            if len(seconds_parts) > 1:
                tenths_str = seconds_parts[1]
                if len(tenths_str) > 0: 
                    microseconds = int(tenths_str.ljust(6, '0')[:6])

            if not (0 <= minutes <= 59 and 0 <= seconds <= 59):
                raise ValueError("Minutos o segundos fuera de rango para un objeto time.")

            return time(hour=0, minute=minutes, second=seconds, microsecond=microseconds)
        except ValueError as e:
            raise ValueError(f"Formato de ModifyDate '{time_str}' no es válido para convertir a time: {e}") from e

    @staticmethod
    def _parse_es_alergenico_str(alergenico_str: str | None) -> bool | None:
        """
        Convierte un string ('TRUE', 'FALSE', 'Unknown') a bool | None.
        Este método es útil si se necesita procesar el string ANTES de pasarlo al constructor.
        """
        if alergenico_str is None:
            return None
        
        valor_limpio_upper = alergenico_str.strip().upper()
        if valor_limpio_upper == 'TRUE':
            return True
        elif valor_limpio_upper == 'FALSE':
            return False
        elif valor_limpio_upper == 'UNKNOWN' or valor_limpio_upper == '':
            return None
        else:
            raise ValueError(f"El valor alergénico '{alergenico_str}' no es válido. Use 'TRUE', 'FALSE', 'Unknown'.")


    @property
    def id_producto(self) -> int:
        return self._id_producto

    @id_producto.setter
    def id_producto(self, valor: int):
        if not isinstance(valor, int):
            raise TypeError("El ID del producto debe ser un número entero.")
        if valor <= 0:
            raise ValueError("El ID del producto debe ser un número positivo.")
        self._id_producto = valor

    @property
    def nombre_producto(self) -> str:
        return self._nombre_producto

    @nombre_producto.setter
    def nombre_producto(self, valor: str):
        if not isinstance(valor, str):
            raise TypeError("El nombre del producto debe ser una cadena de texto.")
        valor_limpio = valor.strip()
        if not valor_limpio:
            raise ValueError("El nombre del producto no puede estar vacío.")
        if len(valor_limpio) < 2:
            raise ValueError("El nombre del producto debe tener al menos 2 caracteres.")
        if len(valor_limpio) > 100:
            raise ValueError("El nombre del producto no puede exceder 100 caracteres.")
        self._nombre_producto = valor_limpio

    @property
    def precio(self) -> Decimal:
        return self._precio

    @precio.setter
    def precio(self, valor: Decimal | float | str):
        try:
            nuevo_precio = Decimal(str(valor))
        except (InvalidOperation, TypeError) as e:
            raise ValueError(f"El precio debe ser un valor numérico válido: {e}")

        if nuevo_precio < Decimal('0'):
            raise ValueError("El precio no puede ser negativo.")
        if nuevo_precio > Decimal('999999.99'):
            raise ValueError("El precio no puede exceder $999,999.99.")
        if nuevo_precio.as_tuple().exponent < -4:
            raise ValueError("El precio no puede tener más de 4 decimales.")
        self._precio = nuevo_precio

    @property
    def id_categoria(self) -> int:
        return self._id_categoria

    @id_categoria.setter
    def id_categoria(self, valor: int):
        if not isinstance(valor, int):
            raise TypeError("El ID de categoría debe ser un número entero.")
        if valor <= 0:
            raise ValueError("El ID de categoría debe ser un número positivo.")
        
        categorias_validas = set(range(1, 12)) 
        if valor not in categorias_validas:
            raise ValueError(f"El ID de categoría {valor} no es válido. IDs válidos: {sorted(list(categorias_validas))}")
        self._id_categoria = valor

    @property
    def clase_producto(self) -> str | None:
        return self._clase_producto

    @clase_producto.setter
    def clase_producto(self, valor: str | None):
        if valor is None:
            self._clase_producto = None
            return
        if not isinstance(valor, str):
            raise TypeError("La clase del producto debe ser una cadena de texto o None.")
        
        valor_limpio = valor.strip()
        if valor_limpio:
            clases_validas = {'Low', 'Medium', 'High'}
            if valor_limpio not in clases_validas:
                raise ValueError(f"La clase '{valor_limpio}' no es válida. Clases válidas: {clases_validas}")
            self._clase_producto = valor_limpio
        else:
            self._clase_producto = None

    @property
    def hora_modificacion(self) -> time | None:
        return self._hora_modificacion

    @hora_modificacion.setter
    def hora_modificacion(self, valor: time | None): # Setter espera time|None
        if valor is None:
            self._hora_modificacion = None
            return
        if not isinstance(valor, time): # Valida que sea un objeto time
            raise TypeError("La hora de modificación debe ser un objeto time o None.")
        self._hora_modificacion = valor

    @property
    def resistente(self) -> str | None:
        return self._resistente

    @resistente.setter
    def resistente(self, valor: str | None):
        if valor is None:
            self._resistente = None
            return
        if not isinstance(valor, str):
            raise TypeError("El nivel de resistencia debe ser una cadena de texto o None.")
        
        valor_limpio = valor.strip()
        if valor_limpio:
            resistencias_validas = {'Durable', 'Weak', 'Unknown'}
            if valor_limpio not in resistencias_validas:
                raise ValueError(f"La resistencia '{valor_limpio}' no es válida. Valores válidos: {resistencias_validas}")
            self._resistente = valor_limpio
        else:
            self._resistente = None

    @property
    def es_alergenico(self) -> bool | None:
        return self._es_alergenico

    @es_alergenico.setter
    def es_alergenico(self, valor: bool | None): # Setter espera bool|None
        if valor is None:
            self._es_alergenico = None
            return
        if not isinstance(valor, bool): # Valida que sea bool
            raise TypeError("El indicador alergénico debe ser un booleano o None.")
        self._es_alergenico = valor

    @property
    def dias_vitalidad(self) -> int | None:
        return self._dias_vitalidad

    @dias_vitalidad.setter
    def dias_vitalidad(self, valor: int | None):
        if valor is None:
            self._dias_vitalidad = None
            return
        
        try:
            valor_entero = int(valor)
        except (ValueError, TypeError):
            raise TypeError("Los días de vitalidad deben ser un número entero o None.")

        if valor_entero < 0:
            raise ValueError("Los días de vitalidad no pueden ser negativos.")
        if valor_entero > 3650:
            raise ValueError("Los días de vitalidad no pueden exceder 3650 días (10 años).")
        self._dias_vitalidad = valor_entero

    def validar_integridad_completa(self) -> bool:
        categorias_perecederas = {2, 4, 6, 7, 10} 
        if self.id_categoria in categorias_perecederas and self.dias_vitalidad is None:
            raise ValueError(
                f"Productos de categoría perecedera (ID: {self.id_categoria}) "
                f"deberían tener días de vitalidad definidos."
            )
        # if self.clase_producto == 'High' and self.precio < Decimal('50'):
        #     raise ValueError("Los productos de clase 'High' deberían tener precio >= $50.")
        # if self.clase_producto == 'Low' and self.precio > Decimal('20'):
        #     raise ValueError("Los productos de clase 'Low' deberían tener precio <= $20.")
        return True

    def obtener_descripcion_detallada(self) -> str:
        partes = [
            f"Producto: {self.nombre_producto}",
            f"Precio: ${self.precio:.2f}",
            f"Categoría ID: {self.id_categoria}"
        ]
        if self.clase_producto: partes.append(f"Clase: {self.clase_producto}")
        if self.dias_vitalidad is not None:
            if self.dias_vitalidad == 0: partes.append("Vitalidad: Producto no perecedero")
            elif self.dias_vitalidad <= 7: partes.append(f"Vitalidad: {self.dias_vitalidad} días (muy perecedero)")
            elif self.dias_vitalidad <= 30: partes.append(f"Vitalidad: {self.dias_vitalidad} días (perecedero)")
            else: partes.append(f"Vitalidad: {self.dias_vitalidad} días (larga duración)")
        if self.es_alergenico is True: partes.append("⚠️ PRODUCTO ALERGÉNICO")
        elif self.es_alergenico is False: partes.append("No alergénico")
        if self.hora_modificacion:
            formatted_time = self.hora_modificacion.strftime('%M:%S')
            if self.hora_modificacion.microsecond > 0:
                decima_segundo = self.hora_modificacion.microsecond // 100000
                if decima_segundo > 0: formatted_time += f".{decima_segundo}"
            partes.append(f"Modificado: {formatted_time}")
        return " | ".join(partes)

    def __str__(self) -> str:
        return f"Producto(ID: {self.id_producto}, Nombre: '{self.nombre_producto}', Precio: ${self.precio:.2f})"

    def __repr__(self) -> str:
        return (f"Producto(id_producto={self.id_producto!r}, nombre_producto={self.nombre_producto!r}, "
                f"precio={self.precio!r}, id_categoria={self.id_categoria!r}, "
                f"clase_producto={self.clase_producto!r}, hora_modificacion={self.hora_modificacion!r}, "
                f"resistente={self.resistente!r}, es_alergenico={self.es_alergenico!r}, "
                f"dias_vitalidad={self.dias_vitalidad!r})")

# --- Ejemplo de uso (para probar la clase) ---
if __name__ == '__main__':
    try:
        # Para instanciar Producto ahora, se necesita pre-procesar algunos datos

        # Pre-procesamiento para producto1
        hm1_str = "21:49.2"
        ea1_str = "Unknown"
        
        hm1_obj = Producto._parse_modify_date_str(hm1_str)
        ea1_bool = Producto._parse_es_alergenico_str(ea1_str)

        producto1 = Producto(
            id_producto=1,
            nombre_producto="Flour - Whole Wheat".strip(), # Asegurar que está limpio
            precio="74.2988", 
            id_categoria=3, # Asumir validado
            clase_producto="Medium", # Asumir validado
            hora_modificacion=hm1_obj, # Pasar objeto time
            resistente="Durable", # Asumir validado
            es_alergenico=ea1_bool, # Pasar bool|None
            dias_vitalidad=0
        )
        print(producto1)
        print(producto1.obtener_descripcion_detallada())
        print(repr(producto1))

        # Pre-procesamiento para producto2
        hm2_obj = time(0, 13, 35, 400000) # Ya es un objeto time
        ea2_bool = True # Ya es un booleano

        producto2 = Producto(
            id_producto=5,
            nombre_producto="Artichokes - Jerusalem".strip(),
            precio=Decimal('65.4771'), 
            id_categoria=2,
            clase_producto="Low",
            hora_modificacion=hm2_obj,
            resistente="Durable",
            es_alergenico=ea2_bool,
            dias_vitalidad=27
        )
        print(producto2)
        print(producto2.obtener_descripcion_detallada())
        
        # Pre-procesamiento para producto_clase_precio
        hm3_str = "23:48.2"
        ea3_str = "TRUE"
        hm3_obj = Producto._parse_modify_date_str(hm3_str)
        ea3_bool = Producto._parse_es_alergenico_str(ea3_str)

        producto_clase_precio = Producto(
             id_producto=28,
             nombre_producto="Sobe - Tropical Energy".strip(),
             precio="12.3153",
             id_categoria=9,
             clase_producto="High", 
             hora_modificacion=hm3_obj,
             resistente="Durable",
             es_alergenico=ea3_bool,
             dias_vitalidad=0
        )
        print(producto_clase_precio.obtener_descripcion_detallada())

        print("\nProbando hora_modificacion con solo segundos:")
        p_test_time = Producto(1000, "Test Time", "10", 1, hora_modificacion=Producto._parse_modify_date_str("15:30"))
        print(p_test_time.obtener_descripcion_detallada())
        assert p_test_time.hora_modificacion == time(0, 15, 30, 0)

    except (TypeError, ValueError) as e:
        print(f"\nError durante la creación del producto o prueba: {e}")