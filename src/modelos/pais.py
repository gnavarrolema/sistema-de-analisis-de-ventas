class Pais:
    """
    Representa un país en el sistema.
    """
    def __init__(self, id_pais: int, nombre_pais: str, codigo_pais: str = None):
        """
        Constructor de la clase Pais.

        Args:
            id_pais (int): El ID único del país.
            nombre_pais (str): El nombre del país.
            codigo_pais (str, optional): El código de dos letras del país. Defaults to None.
        """
        self._id_pais = id_pais
        self._nombre_pais = nombre_pais
        self._codigo_pais = codigo_pais

    @property
    def id_pais(self) -> int:
        return self._id_pais

    @property
    def nombre_pais(self) -> str:
        return self._nombre_pais

    @nombre_pais.setter
    def nombre_pais(self, nuevo_nombre: str):
        if not nuevo_nombre or len(nuevo_nombre.strip()) == 0:
            raise ValueError("El nombre del país no puede estar vacío.")
        self._nombre_pais = nuevo_nombre.strip()

    @property
    def codigo_pais(self) -> str | None: 
        return self._codigo_pais

    @codigo_pais.setter
    def codigo_pais(self, nuevo_codigo: str | None):
        if nuevo_codigo and len(nuevo_codigo.strip()) > 2: # Ejemplo de validación simple
            raise ValueError("El código del país no debe exceder los 2 caracteres.")
        self._codigo_pais = nuevo_codigo.strip() if nuevo_codigo else None

    def __str__(self) -> str:
        return f"País(ID: {self.id_pais}, Nombre: '{self.nombre_pais}', Código: '{self.codigo_pais or ''}')"

    def __repr__(self) -> str:
        return f"Pais(id_pais={self.id_pais!r}, nombre_pais={self.nombre_pais!r}, codigo_pais={self.codigo_pais!r})"