const { pool } = require('./db');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');

async function registerUser(req, res) {
  const { username, email, password } = req.body;

  if (!username || !email || !password) {
    return res.status(400).json({ error: 'Champs requis manquants' });
  }

  try {
    const passwordHash = await bcrypt.hash(password, 10);

    const result = await pool.query(
      `INSERT INTO users (username, email, password_hash)
       VALUES ($1, $2, $3) RETURNING id, username, email`,
      [username, email, passwordHash]
    );

    res.status(201).json({ message: 'Utilisateur créé', user: result.rows[0] });
  } catch (err) {
    if (err.code === '23505') {
      res.status(409).json({ error: 'Nom d’utilisateur ou email déjà utilisé' });
    } else {
      console.error(err);
      res.status(500).json({ error: 'Erreur serveur' });
    }
  }
}

async function loginUser(req, res) {
  const { username, password } = req.body;

  if (!username || !password)
    return res.status(400).json({ error: 'Champs requis manquants' });

  try {
    const result = await pool.query('SELECT * FROM users WHERE username = $1', [username]);

    if (result.rowCount === 0) {
      return res.status(401).json({ error: 'Utilisateur inconnu' });
    }

    const user = result.rows[0];
    const isValid = await bcrypt.compare(password, user.password_hash);

    if (!isValid) {
      return res.status(401).json({ error: 'Mot de passe incorrect' });
    }

    // 🔐 Générer un token
    const token = jwt.sign({ userId: user.id }, process.env.JWT_SECRET, { expiresIn: '2h' });

    // 📦 Stocker en base
    await pool.query(
      'INSERT INTO sessions (user_id, token) VALUES ($1, $2)',
      [user.id, token]
    );

    res.status(200).json({ message: 'Connecté', token });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
}

module.exports = { registerUser, loginUser };

