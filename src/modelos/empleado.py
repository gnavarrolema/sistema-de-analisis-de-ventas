from datetime import date

class Empleado:
    """
    Representa un empleado en el sistema.
    """
    def __init__(self, id_empleado: int, primer_nombre: str, apellido: str, id_ciudad: int,
                 fecha_contratacion: date | None = None, inicial_segundo_nombre: str | None = None,
                 fecha_nacimiento: date | None = None, genero: str | None = None):
        """
        Constructor de la clase Empleado.

        Args:
            id_empleado (int): El ID único del empleado.
            primer_nombre (str): El primer nombre del empleado.
            apellido (str): El apellido del empleado.
            id_ciudad (int): El ID de la ciudad donde reside el empleado.
            fecha_contratacion (date, optional): La fecha de contratación. Defaults to None.
            inicial_segundo_nombre (str, optional): La inicial del segundo nombre. Defaults to None.
            fecha_nacimiento (date, optional): La fecha de nacimiento. Defaults to None.
            genero (str, optional): El género del empleado (ej. 'M', 'F'). Defaults to None.
        """
        self._id_empleado = id_empleado
        self._primer_nombre = primer_nombre
        self._inicial_segundo_nombre = inicial_segundo_nombre
        self._apellido = apellido
        self._fecha_nacimiento = fecha_nacimiento
        self._genero = genero
        self._id_ciudad = id_ciudad
        self._fecha_contratacion = fecha_contratacion

    # --- Propiedades (getters y setters) ---
    @property
    def id_empleado(self) -> int:
        return self._id_empleado

    @property
    def primer_nombre(self) -> str:
        return self._primer_nombre

    @primer_nombre.setter
    def primer_nombre(self, valor: str):
        if not valor or len(valor.strip()) == 0:
            raise ValueError("El primer nombre del empleado no puede estar vacío.")
        self._primer_nombre = valor.strip()

    @property
    def inicial_segundo_nombre(self) -> str | None:
        return self._inicial_segundo_nombre

    @inicial_segundo_nombre.setter
    def inicial_segundo_nombre(self, valor: str | None):
        if valor and len(valor.strip()) > 1:
            self._inicial_segundo_nombre = valor.strip()[0]
        elif valor:
            self._inicial_segundo_nombre = valor.strip()
        else:
            self._inicial_segundo_nombre = None

    @property
    def apellido(self) -> str:
        return self._apellido

    @apellido.setter
    def apellido(self, valor: str):
        if not valor or len(valor.strip()) == 0:
            raise ValueError("El apellido del empleado no puede estar vacío.")
        self._apellido = valor.strip()

    @property
    def fecha_nacimiento(self) -> date | None:
        return self._fecha_nacimiento

    @fecha_nacimiento.setter
    def fecha_nacimiento(self, valor: date | None):
        if valor and not isinstance(valor, date):
            raise TypeError("fecha_nacimiento debe ser un objeto date o None.")
        self._fecha_nacimiento = valor

    @property
    def genero(self) -> str | None:
        return self._genero

    @genero.setter
    def genero(self, valor: str | None):
        if valor and len(valor.strip()) > 1: 
            raise ValueError("El género debe ser un solo carácter o None.")
        self._genero = valor.strip() if valor else None

    @property
    def id_ciudad(self) -> int:
        return self._id_ciudad

    @id_ciudad.setter
    def id_ciudad(self, valor: int):
        # TODO validar si el ID debe ser positivo
        self._id_ciudad = valor

    @property
    def fecha_contratacion(self) -> date | None:
        return self._fecha_contratacion

    @fecha_contratacion.setter
    def fecha_contratacion(self, valor: date | None):
        if valor and not isinstance(valor, date):
            raise TypeError("fecha_contratacion debe ser un objeto date o None.")
        self._fecha_contratacion = valor
        
    # --- Métodos Personalizados ---
    def nombre_completo(self) -> str:
        """
        Devuelve el nombre completo del empleado.
        """
        partes = [self.primer_nombre]
        if self.inicial_segundo_nombre:
            partes.append(self.inicial_segundo_nombre + ".")
        partes.append(self.apellido)
        return " ".join(partes)

    def calcular_antiguedad_anos(self) -> int | None:
        """
        Calcula la antigüedad del empleado en años completos.
        Devuelve None si la fecha de contratación no está definida.
        """
        if not self.fecha_contratacion:
            return None
        
        hoy = date.today() # Obtiene la fecha actual
        anos = hoy.year - self.fecha_contratacion.year
        
        # Ajustar si aún no ha llegado el aniversario de contratación este año
        if (hoy.month, hoy.day) < (self.fecha_contratacion.month, self.fecha_contratacion.day):
            anos -= 1
        
        return anos if anos >= 0 else 0 

    def describir_antiguedad(self) -> str:
        """
        Devuelve una descripción textual de la antigüedad del empleado.
        """
        anos = self.calcular_antiguedad_anos()
        
        if anos is None:
            return "Fecha de contratación no especificada."
        elif anos == 0:
            # Si la antigüedad es menor a un año, se puede considerar como 0 años
            return "Menos de un año de antigüedad."
        elif anos == 1:
            return "1 año de antigüedad."
        else:
            return f"{anos} años de antigüedad."

    # --- Métodos Especiales ---
    def __str__(self) -> str:
        """
        Representación en cadena de texto de un objeto Empleado.
        """
        return f"Empleado(ID: {self.id_empleado}, Nombre: '{self.nombre_completo()}', Contratación: {self.fecha_contratacion})"

    def __repr__(self) -> str:
        """
        Representación oficial del objeto, útil para debugging.
        """
        return (f"Empleado(id_empleado={self.id_empleado!r}, primer_nombre={self.primer_nombre!r}, "
                f"apellido={self.apellido!r}, id_ciudad={self.id_ciudad!r}, "
                f"fecha_contratacion={self.fecha_contratacion!r}, "
                f"inicial_segundo_nombre={self.inicial_segundo_nombre!r}, "
                f"fecha_nacimiento={self.fecha_nacimiento!r}, genero={self.genero!r})")