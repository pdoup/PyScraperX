# Mock API to test functionality
import random

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel


class Book(BaseModel):
    """Pydantic model for a book."""

    title: str
    author: str
    year: int
    genre: str


class Movie(BaseModel):
    """Pydantic model for a movie."""

    title: str
    director: str
    year: int
    genre: str


class Person(BaseModel):
    """Pydantic model for a person."""

    name: str
    age: int
    city: str
    occupation: str


# Initialize the FastAPI application
app = FastAPI(
    title="Random Data API",
    description="A minimal FastAPI application offering random book, movie, and person data.",
    version="1.0.0",
)


# --- Helper functions to generate random data ---
def generate_random_book() -> Book:
    """Generates random book data."""
    titles = [
        "The Great Gatsby",
        "1984",
        "To Kill a Mockingbird",
        "Pride and Prejudice",
        "The Catcher in the Rye",
    ]
    authors = [
        "F. Scott Fitzgerald",
        "George Orwell",
        "Harper Lee",
        "Jane Austen",
        "J.D. Salinger",
    ]
    years = [random.randint(1900, 2023) for _ in range(5)]
    genres = ["Fiction", "Dystopian", "Classic", "Romance", "Coming-of-age"]
    return Book(
        title=random.choice(titles),
        author=random.choice(authors),
        year=random.choice(years),
        genre=random.choice(genres),
    )


def generate_random_movie() -> Movie:
    """Generates random movie data."""
    titles = [
        "Inception",
        "The Matrix",
        "Pulp Fiction",
        "Forrest Gump",
        "Spirited Away",
    ]
    directors = [
        "Christopher Nolan",
        "Lana Wachowski",
        "Quentin Tarantino",
        "Robert Zemeckis",
        "Hayao Miyazaki",
    ]
    years = [random.randint(1980, 2023) for _ in range(5)]
    genres = ["Sci-Fi", "Action", "Drama", "Animation", "Fantasy"]
    return Movie(
        title=random.choice(titles),
        director=random.choice(directors),
        year=random.choice(years),
        genre=random.choice(genres),
    )


def generate_random_person() -> Person:
    """Generates random person data."""
    names = ["Alice Smith", "Bob Johnson", "Charlie Brown", "Diana Prince", "Eve Adams"]
    ages = [random.randint(18, 70) for _ in range(5)]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    occupations = ["Engineer", "Artist", "Doctor", "Teacher", "Chef"]
    return Person(
        name=random.choice(names),
        age=random.choice(ages),
        city=random.choice(cities),
        occupation=random.choice(occupations),
    )


@app.get("/api/book", response_model=Book)
async def get_random_book():
    """
    Returns random book data.
    """
    book_data = generate_random_book()
    return JSONResponse(content=book_data.model_dump())


@app.get("/api/movie", response_model=Movie)
async def get_random_movie():
    """
    Returns random movie data.
    """
    movie_data = generate_random_movie()
    return JSONResponse(content=movie_data.model_dump())


@app.get("/api/person", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person1", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person2", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person3", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person4", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person5", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person6", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person7", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person8", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person9", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person10", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person11", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person12", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person13", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person14", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person15", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person16", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person17", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person18", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person19", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person20", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person21", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person22", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person23", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person24", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person25", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person26", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person27", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person30", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person31", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person32", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person33", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person34", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person35", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())


@app.get("/api/person36", response_model=Person)
async def get_random_person():
    """
    Returns random person data.
    """
    person_data = generate_random_person()
    return JSONResponse(content=person_data.model_dump())
