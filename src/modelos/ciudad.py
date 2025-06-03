class Ciudad:
    """
    Representa una ciudad en el sistema.
    """
    def __init__(self, id_ciudad: int, nombre_ciudad: str, id_pais: int, codigo_postal: str = None):
        """
        Constructor de la clase Ciudad.

        Args:
            id_ciudad (int): El ID único de la ciudad.
            nombre_ciudad (str): El nombre de la ciudad.
            id_pais (int): El ID del país al que pertenece la ciudad.
            codigo_postal (str, optional): El código postal de la ciudad. Defaults to None.
        """
        self._id_ciudad = id_ciudad
        self._nombre_ciudad = nombre_ciudad
        self._codigo_postal = codigo_postal
        self._id_pais = id_pais # Foreign Key a Pais

    @property
    def id_ciudad(self) -> int:
        return self._id_ciudad

    @property
    def nombre_ciudad(self) -> str:
        return self._nombre_ciudad

    @nombre_ciudad.setter
    def nombre_ciudad(self, nuevo_nombre: str):
        if not nuevo_nombre or len(nuevo_nombre.strip()) == 0:
            raise ValueError("El nombre de la ciudad no puede estar vacío.")
        self._nombre_ciudad = nuevo_nombre.strip()

    @property
    def codigo_postal(self) -> str | None:
        return self._codigo_postal

    @codigo_postal.setter
    def codigo_postal(self, nuevo_codigo: str | None):
        self._codigo_postal = nuevo_codigo.strip() if nuevo_codigo else None
        # TODO añadir validaciones específicas del formato del código postal 

    @property
    def id_pais(self) -> int:
        return self._id_pais

    @id_pais.setter
    def id_pais(self, nuevo_id_pais: int):
        # Aquí podrías validar que el id_pais sea un entero positivo, etc.
        self._id_pais = nuevo_id_pais
        
    def __str__(self) -> str:
        return f"Ciudad(ID: {self.id_ciudad}, Nombre: '{self.nombre_ciudad}', CP: '{self.codigo_postal or ''}', ID País: {self.id_pais})"

    def __repr__(self) -> str:
        return (f"Ciudad(id_ciudad={self.id_ciudad!r}, nombre_ciudad={self.nombre_ciudad!r}, "
                f"id_pais={self.id_pais!r}, codigo_postal={self.codigo_postal!r})")