const { Pool } = require('pg');
const dotenv = require('dotenv');
dotenv.config();

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT
});

pool.connect()
  .then(() => console.log('✅ Connexion à la base de données réussie'))
  .catch(err => console.error('❌ Échec de la connexion à la base de données:', err));

module.exports = { pool }; // <<< TRÈS IMPORTANT : Exportez 'pool' dans un objet