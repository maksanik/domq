from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from config import DATABASE_URL

engine = create_async_engine(DATABASE_URL, echo=False)

AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    engine, expire_on_commit=False
)
