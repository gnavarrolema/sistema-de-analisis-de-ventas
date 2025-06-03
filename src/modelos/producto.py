from datetime import time
from decimal import Decimal, InvalidOperation

class Producto:
    """
    Representa un producto en el sistema.
    """
    def __init__(self, id_producto: int, nombre_producto: str, precio: Decimal, id_categoria: int,
                 clase_producto: str | None = None, hora_modificacion: time | None = None,
                 resistente: str | None = None, es_alergenico: str | None = None, 
                 dias_vitalidad: int | None = None): # VitalityDays como int
        """
        Constructor de la clase Producto.

        Args:
            id_producto (int): El ID único del producto.
            nombre_producto (str): El nombre del producto.
            precio (Decimal): El precio del producto.
            id_categoria (int): El ID de la categoría a la que pertenece el producto.
            clase_producto (str, optional): Clase o tipo de producto. Defaults to None.
            hora_modificacion (time, optional): Hora de la última modificación. Defaults to None.
            resistente (str, optional): Nivel de resistencia. Defaults to None.
            es_alergenico (str, optional): Indicador de si es alergénico. Defaults to None.
            dias_vitalidad (int, optional): Días de vitalidad del producto. Defaults to None.
        """
        self._id_producto = id_producto
        self._nombre_producto = nombre_producto
        self.precio = precio 
        self._id_categoria = id_categoria
        self._clase_producto = clase_producto
        self._hora_modificacion = hora_modificacion
        self._resistente = resistente
        self._es_alergenico = es_alergenico
        self.dias_vitalidad = dias_vitalidad

    @property
    def id_producto(self) -> int:
        return self._id_producto

    @property
    def nombre_producto(self) -> str:
        return self._nombre_producto

    @nombre_producto.setter
    def nombre_producto(self, valor: str):
        if not valor or len(valor.strip()) == 0:
            raise ValueError("El nombre del producto no puede estar vacío.")
        self._nombre_producto = valor.strip()

    @property
    def precio(self) -> Decimal:
        return self._precio

    @precio.setter
    def precio(self, valor: Decimal | float | str):
        try:
            nuevo_precio = Decimal(valor)
        except InvalidOperation:
            raise ValueError("El precio debe ser un valor numérico válido.")
        if nuevo_precio < Decimal(0):
            raise ValueError("El precio no puede ser negativo.")
        self._precio = nuevo_precio

    @property
    def id_categoria(self) -> int:
        return self._id_categoria

    @id_categoria.setter
    def id_categoria(self, valor: int):
        self._id_categoria = valor # TODO añadir validación

    @property
    def clase_producto(self) -> str | None:
        return self._clase_producto

    @clase_producto.setter
    def clase_producto(self, valor: str | None):
        self._clase_producto = valor.strip() if valor else None
        
    @property
    def hora_modificacion(self) -> time | None:
        return self._hora_modificacion

    @hora_modificacion.setter
    def hora_modificacion(self, valor: time | None):
        if valor and not isinstance(valor, time):
            raise TypeError("hora_modificacion debe ser un objeto time o None.")
        self._hora_modificacion = valor

    @property
    def resistente(self) -> str | None:
        return self._resistente

    @resistente.setter
    def resistente(self, valor: str | None):
        self._resistente = valor.strip() if valor else None

    @property
    def es_alergenico(self) -> str | None: 
        return self._es_alergenico

    @es_alergenico.setter
    def es_alergenico(self, valor: str | None):
        self._es_alergenico = valor.strip() if valor else None

    @property
    def dias_vitalidad(self) -> int | None:
        return self._dias_vitalidad

    @dias_vitalidad.setter
    def dias_vitalidad(self, valor: int | None):
        if valor is not None:
            if not isinstance(valor, int):
                try:
                    valor = int(valor)
                except ValueError:
                    raise TypeError("dias_vitalidad debe ser un entero o None.")
            if valor < 0:
                raise ValueError("dias_vitalidad no puede ser negativo.")
        self._dias_vitalidad = valor
        
    def __str__(self) -> str:
        return f"Producto(ID: {self.id_producto}, Nombre: '{self.nombre_producto}', Precio: {self.precio:.2f})"

    def __repr__(self) -> str:
        return (f"Producto(id_producto={self.id_producto!r}, nombre_producto={self.nombre_producto!r}, "
                f"precio={self.precio!r}, id_categoria={self.id_categoria!r})")