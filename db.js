const { Pool } = require('pg');
const dotenv = require('dotenv');
dotenv.config();

const isProduction = process.env.NODE_ENV === 'production';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: isProduction ? { rejectUnauthorized: false } : false,
});

pool.connect()
  .then(() => console.log('✅ Connexion à la base de données réussie'))
  .catch(err => console.error('❌ Échec de la connexion à la base de données:', err));

module.exports = { pool };
