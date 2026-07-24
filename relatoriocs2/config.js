/* Backend das edições compartilhadas (projeto stark-admin na Vercel).
   As edições (observação, cor, linhas novas, linhas ocultas) ficam em
   relatoriocs2/overlays.json no repo vesti-mobi/dados — todo mundo vê o mesmo.

   Se apagar o valor daqui (window.CS2_API = ""), o painel volta a salvar só no
   navegador de quem editou, sem quebrar nada. */
window.CS2_API = "https://stark-admin-phi.vercel.app";
