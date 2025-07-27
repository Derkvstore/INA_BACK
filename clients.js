// clients.js
const express = require('express');
const router = express.Router();
const { pool } = require('./db'); // <<< CORRECTION ICI

// Route GET pour lister tous les clients
router.get('/', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM clients ORDER BY id DESC');
    res.json(result.rows);
  } catch (err) {
    console.error("Erreur GET clients:", err); // Ajout d'un log plus spécifique
    res.status(500).json({ error: 'Erreur serveur lors de la récupération des clients.' });
  }
});

// Route POST pour ajouter un nouveau client
router.post('/', async (req, res) => {
  const { nom, telephone, adresse } = req.body;
  if (!nom) {
    return res.status(400).json({ error: 'Le nom du client est requis.' });
  }
  try {
    const result = await pool.query(
      'INSERT INTO clients (nom, telephone, adresse) VALUES ($1, $2, $3) RETURNING *',
      [nom, telephone || null, adresse || null]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    if (err.code === '23505') { // Code d'erreur pour violation de contrainte unique
      return res.status(409).json({ error: 'Un client avec ce nom existe déjà.' });
    }
    console.error("Erreur POST clients:", err);
    res.status(500).json({ error: 'Erreur serveur lors de l\'ajout du client.' });
  }
});

// Route PUT pour modifier un client existant
router.put('/:id', async (req, res) => {
  const { id } = req.params;
  const { nom, telephone, adresse } = req.body;
  if (!nom) {
    return res.status(400).json({ error: 'Le nom du client est requis pour la mise à jour.' });
  }
  try {
    const result = await pool.query(
      'UPDATE clients SET nom = $1, telephone = $2, adresse = $3 WHERE id = $4 RETURNING *',
      [nom, telephone || null, adresse || null, id]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Client non trouvé.' });
    }
    res.json(result.rows[0]);
  } catch (err) {
    if (err.code === '23505') {
      return res.status(409).json({ error: 'Un client avec ce nom existe déjà.' });
    }
    console.error("Erreur PUT clients:", err);
    res.status(500).json({ error: 'Erreur serveur lors de la mise à jour du client.' });
  }
});

// Route DELETE pour supprimer un client
router.delete('/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query('DELETE FROM clients WHERE id = $1 RETURNING *', [id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Client non trouvé.' });
    }
    res.status(204).send(); // No Content
  } catch (err) {
    console.error("Erreur DELETE clients:", err);
    res.status(500).json({ error: 'Erreur serveur lors de la suppression du client.' });
  }
});

module.exports = router;