# DOS-C Wire Format Specification

## Overview
This document specifies the minimal DOS-C (Directory of Service - Client) wire format sent over TCP from Cloud-Node server to clients.

## Design Goals

1. **Minimal Size**: Reduce wire format by ~70% by excluding server-side metadata
2. **Self-Exclusion**: Each client receives DOS-C without their own entry
3. **Cover Image Separation**: Cover images stored server-side only, not transmitted in DOS-C
4. **Actual Images Only**: DOS-C contains only shareable images, not cover images

## Version History

| Version | Description | Size per Client (avg) |
|---------|-------------|------------------------|
| v1.0 | Original format with IP, port, timestamps, online status | ~85 bytes |
| v2.0 | Minimal format with name + actual images only | ~25 bytes |

**Size Reduction**: ~70% smaller

## v2.0 Format (Current)

### Packet Structure

```
[DOS Version: u64 (8 bytes)]
[Number of Clients: u32 (4 bytes)]
[Client Entry 1]
[Client Entry 2]
...
[Client Entry N]
```

### Client Entry Structure

```
[Name Length: u16 (2 bytes)]
[Name: UTF-8 string (variable)]
[Number of Images: u32 (4 bytes)]
[Image Entry 1]
[Image Entry 2]
...
[Image Entry M]
```

### Image Entry Structure

```
[Image Name Length: u16 (2 bytes)]
[Image Name: UTF-8 string (variable)]
```

## Example Encoding

**Scenario**: Server sends DOS-C to client "alice". Server has 3 clients:
- alice (EXCLUDED from alice's DOS-C)
- bob with images: ["sunset.jpg", "beach.png"]
- charlie with images: ["mountain.jpg"]

**Wire Format sent to alice**:

```
Offset | Bytes                      | Field
-------|----------------------------|----------------------------------
0x00   | 01 00 00 00 00 00 00 00   | DOS Version = 1 (u64 LE)
0x08   | 02 00 00 00               | Num Clients = 2 (u32 LE) - alice excluded!
       |                            |
       | --- Client 1: bob ---      |
0x0C   | 03 00                      | Name Length = 3 (u16 LE)
0x0E   | 62 6F 62                   | Name = "bob"
0x11   | 02 00 00 00               | Num Images = 2 (u32 LE)
0x15   | 0A 00                      | Image 1 Length = 10 (u16 LE)
0x17   | 73 75 6E 73 65 74 2E 6A...|  "sunset.jpg"
0x21   | 09 00                      | Image 2 Length = 9 (u16 LE)
0x23   | 62 65 61 63 68 2E 70 6E...| "beach.png"
       |                            |
       | --- Client 2: charlie ---  |
0x2C   | 07 00                      | Name Length = 7 (u16 LE)
0x2E   | 63 68 61 72 6C 69 65       | Name = "charlie"
0x35   | 01 00 00 00               | Num Images = 1 (u32 LE)
0x39   | 0C 00                      | Image 1 Length = 12 (u16 LE)
0x3B   | 6D 6F 75 6E 74 61 69 6E...| "mountain.jpg"
```

**Total Size**: ~71 bytes (vs ~255 bytes with v1.0 format for 3 clients)

## Excluded Fields (Server-Side Only)

The following fields are maintained in DOS-S (server-side directory) but NOT sent in DOS-C:

- **client_ip** (String) - Client IP address
- **client_port** (u16) - Client port number
- **cover_image** (Option\<String>) - Cover image filename
- **last_seen** (u64) - Unix timestamp of last activity
- **online** (bool) - Online/offline status

These fields are used for:
- Server-to-server communication
- P2P connection establishment (server provides IP/port when needed)
- Server monitoring and cleanup

## Self-Exclusion Filter

**Rule**: Server MUST filter out the requesting client from DOS-C before sending.

**Rationale**:
- Client already knows its own images
- Reduces bandwidth
- Prevents confusing UX (requesting your own image)

**Implementation** (server-side):
```rust
let clients_to_send: Vec<_> = state.dos_clients
    .iter()
    .filter(|(name, _)| *name != &username)  // Exclude requesting client
    .collect();
```

## Cover Image Model

**Rule**: Each client has ONE cover image that is applied to ALL its actual images.

**Storage**:
- Server stores: `cover_image: Option<String>` in DOS-S
- Client stores: `cover_image: Option<String>` in local state
- Wire format: Cover image NOT transmitted in DOS-C

**Command Line Syntax**:
```bash
cargo run -- interactive alice server:port COVER.png photo1.jpg photo2.jpg
```
- First image = cover (not shareable, used for steganography)
- Remaining images = actual images (shareable, shown in DOS-C)

**JOIN Message Format** (client→server):
```
[username_len:u16][username]
[port:u16]
[num_images:u32]
[image1_len:u16][image1]  ← Cover image (if num_images > 1)
[image2_len:u16][image2]  ← Actual image 1
[image3_len:u16][image3]  ← Actual image 2
...
```

**Server Interpretation**:
- If `num_images > 1`: First image = cover, rest = actual
- If `num_images == 1`: Single image = actual, no cover

## Parsing Algorithm (Client-Side)

```rust
pub fn parse_dos_c_from_join_ack(payload: &[u8]) -> Result<(HashMap<String, DosClient>, u64)> {
    let mut offset = 0;

    // Parse version
    let dos_version = u64::from_le_bytes(payload[offset..offset+8].try_into()?);
    offset += 8;

    // Parse num_clients
    let num_clients = u32::from_le_bytes(payload[offset..offset+4].try_into()?) as usize;
    offset += 4;

    let mut clients = HashMap::new();

    for _ in 0..num_clients {
        // Parse name
        let name_len = u16::from_le_bytes(payload[offset..offset+2].try_into()?) as usize;
        offset += 2;
        let name = String::from_utf8(payload[offset..offset+name_len].to_vec())?;
        offset += name_len;

        // Parse images
        let num_images = u32::from_le_bytes(payload[offset..offset+4].try_into()?) as usize;
        offset += 4;

        let mut images = Vec::new();
        for _ in 0..num_images {
            let img_len = u16::from_le_bytes(payload[offset..offset+2].try_into()?) as usize;
            offset += 2;
            let image = String::from_utf8(payload[offset..offset+img_len].to_vec())?;
            offset += img_len;
            images.push(image);
        }

        clients.insert(name.clone(), DosClient {
            client_name: name,
            actual_images: images,
        });
    }

    Ok((clients, dos_version))
}
```

## Building Algorithm (Server-Side)

```rust
// Build minimal DOS-C payload
fn build_dos_c_payload(state: &ServerState, requesting_client: &str) -> Vec<u8> {
    let mut payload = Vec::new();

    // DOS version (u64)
    payload.extend((state.dos_c_version as u64).to_le_bytes());

    // Filter out requesting client (self-exclusion)
    let clients_to_send: Vec<_> = state.dos_clients
        .iter()
        .filter(|(name, _)| *name != requesting_client)
        .collect();

    // Num clients (u32)
    payload.extend((clients_to_send.len() as u32).to_le_bytes());

    // Client entries
    for (name, client) in clients_to_send {
        // Name
        payload.extend((name.len() as u16).to_le_bytes());
        payload.extend_from_slice(name.as_bytes());

        // Actual images count (exclude cover)
        payload.extend((client.actual_images.len() as u32).to_le_bytes());

        // Image entries (actual images only, no cover)
        for img in &client.actual_images {
            payload.extend((img.len() as u16).to_le_bytes());
            payload.extend_from_slice(img.as_bytes());
        }
    }

    payload
}
```

## Migration from v1.0 to v2.0

| Field (v1.0) | v2.0 Status | Notes |
|--------------|-------------|-------|
| dos_version | ✅ Kept | Changed to u64 (was u32) |
| num_clients | ✅ Kept | Now excludes requesting client |
| client_name | ✅ Kept | Unchanged |
| client_ip | ❌ Removed | Server-side only |
| client_port | ❌ Removed | Server-side only |
| images | ✅ Modified | Split into cover + actual, only actual sent |
| last_seen | ❌ Removed | Server-side only |
| online | ❌ Removed | Server-side only |

## Testing Checklist

- [ ] DOS-C excludes requesting client (alice doesn't see alice)
- [ ] Cover image not transmitted in DOS-C
- [ ] Only actual images appear in DOS-C
- [ ] Wire format size ~70% smaller than v1.0
- [ ] Multiple clients with varying image counts parse correctly
- [ ] Empty image lists handled correctly
- [ ] UTF-8 client names and image names supported
- [ ] Version increments trigger client refresh

## Future Enhancements (Not in v2.0)

- **Compression**: gzip DOS-C payload for large networks
- **Delta Updates**: Send only changed clients instead of full DOS-C
- **Filtering**: Client-side filtering by online status, image type, etc.
- **Pagination**: For networks with >1000 clients
