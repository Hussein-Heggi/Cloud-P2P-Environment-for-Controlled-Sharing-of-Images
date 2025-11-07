// app.js

const loginCard   = document.getElementById("loginCard");
const uploadCard  = document.getElementById("uploadCard");
const resultCard  = document.getElementById("resultCard");

const loginForm   = document.getElementById("loginForm");
const runForm     = document.getElementById("runForm");
const runStatus   = document.getElementById("runStatus");
const backBtn     = document.getElementById("backBtn");

const resultImage = document.getElementById("resultImage");
const decryptedList = document.getElementById("decryptedList");

const API = "http://localhost:3000/api";

// Dummy login
loginForm.addEventListener("submit", (e) => {
  e.preventDefault();
  const username = document.getElementById("username").value.trim();
  const password = document.getElementById("password").value.trim();
  if (!username || !password) return;
  loginCard.classList.add("hidden");
  uploadCard.classList.remove("hidden");
  clearResult();
});

// Run client with uploaded PNG + metadata
runForm.addEventListener("submit", async (e) => {
  e.preventDefault();

  const imgInput = document.getElementById("image");
  const meta = document.getElementById("metadata").value.trim();

  if (!imgInput.files.length) {
    alert("Please choose a PNG image");
    return;
  }
  if (!/^[A-Za-z0-9_]+:\d+(,[A-Za-z0-9_]+:\d+)*$/.test(meta)) {
    alert("Metadata must be like bob:5 or alice:3,bob:2");
    return;
  }

  const fd = new FormData();
  fd.append("username", "demo");
  fd.append("password", "demo");
  fd.append("metadata", meta);
  fd.append("image", imgInput.files[0]);

  runStatus.textContent = "Running client…";
  toggleRunDisabled(true);

  try {
    const resp = await fetch(`${API}/run-client`, {
      method: "POST",
      body: fd
    });
    const data = await resp.json();

    runStatus.textContent = resp.ok
      ? `Done in ${data.duration_ms} ms.`
      : (data.error || "Client finished with error.");

    if (data.image_url) {
      const absolute = `http://localhost:3000${data.image_url}`;
      resultImage.src = absolute;
    } else {
      resultImage.removeAttribute("src");
    }

    decryptedList.innerHTML = "";
    (data.decrypted_files || []).forEach(f => {
      const div = document.createElement("div");
      div.className = "file";
      const title = document.createElement("h3");
      title.textContent = `${f.file} (${f.size} bytes)`;
      const pre = document.createElement("pre");
      pre.textContent = f.content;
      div.appendChild(title);
      div.appendChild(pre);
      decryptedList.appendChild(div);
    });

    uploadCard.classList.add("hidden");
    resultCard.classList.remove("hidden");
  } catch (e2) {
    console.error(e2);
    runStatus.textContent = "Failed to contact backend.";
  } finally {
    toggleRunDisabled(false);
  }
});

// Back button to retry
backBtn.addEventListener("click", () => {
  clearResult();
  resultCard.classList.add("hidden");
  uploadCard.classList.remove("hidden");
  runForm.reset();
  runStatus.textContent = "";
});

function toggleRunDisabled(disabled) {
  [...runForm.querySelectorAll("input,button")].forEach(el => el.disabled = disabled);
}

function clearResult() {
  resultImage.removeAttribute("src");
  decryptedList.innerHTML = "";
}
