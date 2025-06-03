from datetime import time
from decimal import Decimal, InvalidOperation

class Venta:
    """
    Representa una transacción de venta en el sistema.
    """
    def __init__(self, id_venta: int, id_producto: int, id_cliente: int, cantidad: int, 
                 precio_total: Decimal, id_empleado_vendedor: int | None = None, 
                 descuento: Decimal | None = None, hora_venta: time | None = None, # Cambiado SalesDate a hora_venta
                 numero_transaccion: str | None = None):
        """
        Constructor de la clase Venta.

        Args:
            id_venta (int): El ID único de la venta.
            id_producto (int): El ID del producto vendido.
            id_cliente (int): El ID del cliente que realizó la compra.
            cantidad (int): La cantidad de producto vendido.
            precio_total (Decimal): El precio total de la venta.
            id_empleado_vendedor (int, optional): ID del empleado que realizó la venta. Defaults to None.
            descuento (Decimal, optional): El descuento aplicado a la venta. Defaults to None.
            hora_venta (time, optional): La hora en que se realizó la venta. Defaults to None.
            numero_transaccion (str, optional): Número de referencia de la transacción. Defaults to None.
        """
        self._id_venta = id_venta
        self._id_empleado_vendedor = id_empleado_vendedor
        self._id_producto = id_producto
        self._id_cliente = id_cliente
        self.cantidad = cantidad # Usar setter
        self.descuento = descuento # Usar setter
        self.precio_total = precio_total # Usar setter
        self._hora_venta = hora_venta
        self._numero_transaccion = numero_transaccion

    @property
    def id_venta(self) -> int:
        return self._id_venta

    @property
    def id_empleado_vendedor(self) -> int | None:
        return self._id_empleado_vendedor

    @id_empleado_vendedor.setter
    def id_empleado_vendedor(self, valor: int | None):
        self._id_empleado_vendedor = valor # TODO añadir validación

    @property
    def id_producto(self) -> int:
        return self._id_producto

    @id_producto.setter
    def id_producto(self, valor: int):
        self._id_producto = valor # TODO añadir validación

    @property
    def id_cliente(self) -> int:
        return self._id_cliente

    @id_cliente.setter
    def id_cliente(self, valor: int):
        self._id_cliente = valor # TODO añadir validación

    @property
    def cantidad(self) -> int:
        return self._cantidad

    @cantidad.setter
    def cantidad(self, valor: int):
        if not isinstance(valor, int) or valor <= 0:
            raise ValueError("La cantidad debe ser un entero positivo.")
        self._cantidad = valor

    @property
    def descuento(self) -> Decimal | None:
        return self._descuento

    @descuento.setter
    def descuento(self, valor: Decimal | float | str | None):
        if valor is None:
            self._descuento = None
            return
        try:
            nuevo_descuento = Decimal(valor)
        except InvalidOperation:
            raise ValueError("El descuento debe ser un valor numérico válido o None.")
        if nuevo_descuento < Decimal(0): # Asumiendo que el descuento no puede ser negativo
            raise ValueError("El descuento no puede ser negativo.")
        self._descuento = nuevo_descuento

    @property
    def precio_total(self) -> Decimal:
        return self._precio_total

    @precio_total.setter
    def precio_total(self, valor: Decimal | float | str):
        try:
            nuevo_precio_total = Decimal(valor)
        except InvalidOperation:
            raise ValueError("El precio total debe ser un valor numérico válido.")
        # TODO validar que precio_total sea >= 0, o incluso que sea consistente
        # TODOcon cantidad * precio_producto - descuento, pero eso requeriría más lógica.
        self._precio_total = nuevo_precio_total

    @property
    def hora_venta(self) -> time | None:
        return self._hora_venta

    @hora_venta.setter
    def hora_venta(self, valor: time | None):
        if valor and not isinstance(valor, time):
            raise TypeError("hora_venta debe ser un objeto time o None.")
        self._hora_venta = valor

    @property
    def numero_transaccion(self) -> str | None:
        return self._numero_transaccion

    @numero_transaccion.setter
    def numero_transaccion(self, valor: str | None):
        self._numero_transaccion = valor.strip() if valor else None

    def __str__(self) -> str:
        return (f"Venta(ID: {self.id_venta}, ProductoID: {self.id_producto}, ClienteID: {self.id_cliente}, "
                f"Total: {self.precio_total:.2f}, Hora: {self.hora_venta})")

    def __repr__(self) -> str:
        return (f"Venta(id_venta={self.id_venta!r}, id_producto={self.id_producto!r}, "
                f"id_cliente={self.id_cliente!r}, precio_total={self.precio_total!r}, "
                f"hora_venta={self.hora_venta!r})")