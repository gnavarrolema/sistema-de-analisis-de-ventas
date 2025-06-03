class Cliente:
    """
    Representa un cliente en el sistema.
    """
    def __init__(self, id_cliente: int, primer_nombre: str, apellido: str, 
                 id_ciudad: int, inicial_segundo_nombre: str = None, direccion: str = None):
        """
        Constructor de la clase Cliente.

        Args:
            id_cliente (int): El ID único del cliente.
            primer_nombre (str): El primer nombre del cliente.
            apellido (str): El apellido del cliente.
            id_ciudad (int): El ID de la ciudad donde reside el cliente.
            inicial_segundo_nombre (str, optional): La inicial del segundo nombre. Defaults to None.
            direccion (str, optional): La dirección del cliente. Defaults to None.
        """
        self._id_cliente = id_cliente
        self._primer_nombre = primer_nombre
        self._inicial_segundo_nombre = inicial_segundo_nombre
        self._apellido = apellido
        self._direccion = direccion
        self._id_ciudad = id_ciudad # Foreign Key a Ciudad

    @property
    def id_cliente(self) -> int:
        return self._id_cliente

    @property
    def primer_nombre(self) -> str:
        return self._primer_nombre

    @primer_nombre.setter
    def primer_nombre(self, nuevo_nombre: str):
        if not nuevo_nombre or len(nuevo_nombre.strip()) == 0:
            raise ValueError("El primer nombre del cliente no puede estar vacío.")
        self._primer_nombre = nuevo_nombre.strip()

    @property
    def inicial_segundo_nombre(self) -> str | None:
        return self._inicial_segundo_nombre

    @inicial_segundo_nombre.setter
    def inicial_segundo_nombre(self, nueva_inicial: str | None):
        if nueva_inicial and len(nueva_inicial.strip()) > 1:
            # Podríamos ser más estrictos o limpiar la entrada
            self._inicial_segundo_nombre = nueva_inicial.strip()[0] 
        elif nueva_inicial:
             self._inicial_segundo_nombre = nueva_inicial.strip()
        else:
            self._inicial_segundo_nombre = None


    @property
    def apellido(self) -> str:
        return self._apellido

    @apellido.setter
    def apellido(self, nuevo_apellido: str):
        if not nuevo_apellido or len(nuevo_apellido.strip()) == 0:
            raise ValueError("El apellido del cliente no puede estar vacío.")
        self._apellido = nuevo_apellido.strip()

    @property
    def direccion(self) -> str | None:
        return self._direccion

    @direccion.setter
    def direccion(self, nueva_direccion: str | None):
        self._direccion = nueva_direccion.strip() if nueva_direccion else None

    @property
    def id_ciudad(self) -> int:
        return self._id_ciudad

    @id_ciudad.setter
    def id_ciudad(self, nuevo_id_ciudad: int):
        self._id_ciudad = nuevo_id_ciudad

    def nombre_completo(self) -> str:
        """
        Devuelve el nombre completo del cliente.
        Este es un ejemplo de un método personalizado relevante para el negocio.
        """
        partes_nombre = [self.primer_nombre]
        if self.inicial_segundo_nombre:
            partes_nombre.append(self.inicial_segundo_nombre + ".")
        partes_nombre.append(self.apellido)
        return " ".join(partes_nombre)

    def __str__(self) -> str:
        return f"Cliente(ID: {self.id_cliente}, Nombre: '{self.nombre_completo()}')"

    def __repr__(self) -> str:
        return (f"Cliente(id_cliente={self.id_cliente!r}, primer_nombre={self.primer_nombre!r}, "
                f"inicial_segundo_nombre={self.inicial_segundo_nombre!r}, apellido={self.apellido!r}, "
                f"id_ciudad={self.id_ciudad!r}, direccion={self.direccion!r})")