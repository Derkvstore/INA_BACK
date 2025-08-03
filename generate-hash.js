const bcrypt = require('bcrypt');

bcrypt.hash('Ina8202', 10).then(hash => {
  console.log('Hash généré :', hash);
});
