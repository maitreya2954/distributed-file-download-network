"""Initial migration

Revision ID: 102f05b040c3
Revises: 
Create Date: 2023-04-09 17:30:02.808600

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '102f05b040c3'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('node',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('ip', sa.String(length=20), nullable=False),
    sa.Column('port', sa.String(length=5), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('node')
    # ### end Alembic commands ###