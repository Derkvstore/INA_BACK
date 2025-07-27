// auth.js
const { pool } = require('./db'); // <<< CORRECT ICI
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');

async function registerUser(req, res) {
  const { username, email, password, full_name } = req.body;

  if (!username || !email || !password || !full_name) {
    return res.status(400).json({ error: 'Champs requis manquants' });
  }

  try {
    const passwordHash = await bcrypt.hash(password, 10);

    const result = await pool.query(
      `INSERT INTO users (username, email, password_hash, full_name)
       VALUES ($1, $2, $3, $4) RETURNING id, username, email, full_name`,
      [username, email, passwordHash, full_name]
    );

    res.status(201).json({ message: 'Utilisateur créé', user: result.rows[0] });
  } catch (err) {
    if (err.code === '23505') {
      res.status(409).json({ error: 'Nom d’utilisateur ou email déjà utilisé' });
    } else {
      console.error('Erreur lors de l\'enregistrement :', err);
      res.status(500).json({ error: 'Erreur serveur' });
    }
  }
}

async function loginUser(req, res) {
  const { username, password } = req.body;
  console.log('Tentative de connexion pour :', username);

  if (!username || !password) {
    return res.status(400).json({ error: 'Champs requis manquants' });
  }

  try {
    const result = await pool.query('SELECT id, username, password_hash, full_name FROM users WHERE username = $1', [username]);

    if (result.rowCount === 0) {
      return res.status(401).json({ error: 'Utilisateur inconnu' });
    }

    const user = result.rows[0];
    const validPassword = await bcrypt.compare(password, user.password_hash);

    if (!validPassword) {
      return res.status(401).json({ error: 'Mot de passe incorrect' });
    }

    const token = jwt.sign({ userId: user.id }, process.env.JWT_SECRET, { expiresIn: '1h' });

    // Stockez la session si vous le souhaitez (votre code original)
    // await pool.query('INSERT INTO sessions (user_id, token) VALUES ($1, $2)', [user.id, token]);

    res.json({ message: 'Connecté', token, fullName: user.full_name, username: user.username });
  } catch (err) {
    console.error('Erreur login :', err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
}

module.exports = { registerUser, loginUser };