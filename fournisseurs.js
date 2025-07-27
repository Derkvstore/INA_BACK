// backend/routes/fournisseurs.js
const express = require('express');
const router = express.Router();
const { pool } = require('./db'); // Assurez-vous que le chemin vers votre pool de connexion est correct

// Récupérer tous les fournisseurs
router.get('/', async (req, res) => {
  try {
    const result = await pool.query('SELECT id, nom, telephone, adresse, date_ajout FROM fournisseurs ORDER BY nom');
    res.status(200).json(result.rows);
  } catch (error) {
    console.error('Erreur lors de la récupération des fournisseurs:', error);
    res.status(500).json({ error: 'Erreur serveur lors de la récupération des fournisseurs.' });
  }
});

// Récupérer un fournisseur par ID
router.get('/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query('SELECT id, nom, telephone, adresse, date_ajout FROM fournisseurs WHERE id = $1', [id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Fournisseur non trouvé.' });
    }
    res.status(200).json(result.rows[0]);
  } catch (error) {
    console.error('Erreur lors de la récupération du fournisseur par ID:', error);
    res.status(500).json({ error: 'Erreur serveur lors de la récupération du fournisseur.' });
  }
});

// Ajouter un nouveau fournisseur
router.post('/', async (req, res) => {
  const { nom, telephone, adresse } = req.body;
  if (!nom) {
    return res.status(400).json({ error: 'Le nom du fournisseur est obligatoire.' });
  }
  try {
    const result = await pool.query(
      'INSERT INTO fournisseurs (nom, telephone, adresse) VALUES ($1, $2, $3) RETURNING *',
      [nom, telephone || null, adresse || null]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error('Erreur lors de l\'ajout du fournisseur:', error);
    if (error.code === '23505') { // Code pour violation de contrainte unique
      return res.status(409).json({ error: 'Un fournisseur avec ce nom existe déjà.' });
    }
    res.status(500).json({ error: 'Erreur serveur lors de l\'ajout du fournisseur.' });
  }
});

// Mettre à jour un fournisseur existant
router.put('/:id', async (req, res) => {
  const { id } = req.params;
  const { nom, telephone, adresse } = req.body;
  if (!nom) {
    return res.status(400).json({ error: 'Le nom du fournisseur est obligatoire.' });
  }
  try {
    const result = await pool.query(
      'UPDATE fournisseurs SET nom = $1, telephone = $2, adresse = $3 WHERE id = $4 RETURNING *',
      [nom, telephone || null, adresse || null, id]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Fournisseur non trouvé.' });
    }
    res.status(200).json(result.rows[0]);
  } catch (error) {
    console.error('Erreur lors de la mise à jour du fournisseur:', error);
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Un fournisseur avec ce nom existe déjà.' });
    }
    res.status(500).json({ error: 'Erreur serveur lors de la mise à jour du fournisseur.' });
  }
});

// Supprimer un fournisseur
router.delete('/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query('DELETE FROM fournisseurs WHERE id = $1 RETURNING *', [id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Fournisseur non trouvé.' });
    }
    res.status(200).json({ message: 'Fournisseur supprimé avec succès.', deletedFournisseur: result.rows[0] });
  } catch (error) {
    console.error('Erreur lors de la suppression du fournisseur:', error);
    if (error.code === '23503') { // Code pour violation de clé étrangère
      return res.status(409).json({ error: 'Impossible de supprimer ce fournisseur car il est lié à un ou plusieurs produits.' });
    }
    res.status(500).json({ error: 'Erreur serveur lors de la suppression du fournisseur.' });
  }
});

module.exports = router;