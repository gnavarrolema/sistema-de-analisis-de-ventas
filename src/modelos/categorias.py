class Categoria:
    """
    Representa una categoría de producto en el sistema.
    """
    def __init__(self, id_categoria: int, nombre_categoria: str):
        """
        Constructor de la clase Categoria.

        Args:
            id_categoria (int): El ID único de la categoría.
            nombre_categoria (str): El nombre de la categoría.
        """
        self._id_categoria = id_categoria  
        self._nombre_categoria = nombre_categoria

    # Métodos "getter" para acceder a los atributos (Encapsulamiento)
    @property
    def id_categoria(self) -> int:
        return self._id_categoria

    @property
    def nombre_categoria(self) -> str:
        return self._nombre_categoria

    # Métodos "setter" para permitir la modificación controlada
    @nombre_categoria.setter
    def nombre_categoria(self, nuevo_nombre: str):
        if not nuevo_nombre or len(nuevo_nombre.strip()) == 0:
            raise ValueError("El nombre de la categoría no puede estar vacío.")
        self._nombre_categoria = nuevo_nombre.strip()

    def __str__(self) -> str:
        """
        Representación en cadena de texto de un objeto Categoria.
        """
        return f"Categoría(ID: {self.id_categoria}, Nombre: '{self.nombre_categoria}')"

    def __repr__(self) -> str:
        """
        Representación oficial del objeto, útil para debugging.
        """
        return f"Categoria(id_categoria={self.id_categoria!r}, nombre_categoria={self.nombre_categoria!r})"