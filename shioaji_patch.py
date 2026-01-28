
# =========================================================
# 🩹 Monkeypatch pysolace to fix IndexError in fetch_contracts
#    Cause: shioaji/pysolace C-extension buffer overflow with page_size=5000
#    Fix: 1. Intercept LOW-LEVEL request at _core level and reduce page_size to 100
#         2. Re-implement onreply_callback_wrap to SUPPRESS IndexError (loguru catches it otherwise)
# =========================================================

import threading

# --- NEW: Global event to signal Token Expiration ---
_GLOBAL_TOKEN_EXPIRED_EVENT = threading.Event()

try:
    import pysolace
    import pysolace._core as _core_module
    
    # Avoid patching twice
    if not getattr(_core_module, "_patched_by_bot", False):
        _original_core_request = _core_module.request

        def patched_core_request(*args):
            # Signature: request(sol_ptr, topic, corrid, payload, qos, cos, format)
            # We expect at least 4 args, with payload at index 3
            if len(args) >= 4:
                payload = args[3]
                if isinstance(payload, dict) and "page_size" in payload:
                    # Aggressively reduce to 100 to prevent buffer overflow
                    if payload["page_size"] > 100:
                        payload["page_size"] = 100
            return _original_core_request(*args)

        _core_module.request = patched_core_request
        _core_module._patched_by_bot = True
        pysolace.solclient.request = patched_core_request
        
        # ---------------------------------------------------------
        # Re-implement onreply_callback_wrap to bypass loguru catch
        # ---------------------------------------------------------
        def patched_onreply_callback_wrap(self, topic: str, corrid: str, reply: dict):
            try:
                # 1. Update response dict in map
                if corrid in self.req_rep_map:
                    # Update the dict in place so 'request' function sees changes
                    for k, v in reply.items():
                        self.req_rep_map[corrid][k] = v
                
                # 2. Signal completion event
                if corrid in self.rep_event_map:
                    recv = self.rep_event_map.pop(corrid)
                    recv.set()
                
                # 3. Cleanup map
                if corrid in self.req_rep_map:
                    self.req_rep_map.pop(corrid)
                
                # 4. Execute Callback (The source of IndexError)
                if corrid in self.rep_callback_map:
                    rep_cb = self.rep_callback_map.pop(corrid)
                    if rep_cb:
                        try:
                            # This calls api.SolaceAPI._fetch_contracts_cb
                            rep_cb(topic, corrid, reply)
                        except IndexError:
                            # Supress the specific crash
                            pass
                        except ValueError as e:
                            # Supress "'' is not a valid SecurityType"
                            if "SecurityType" in str(e):
                                pass
                            else:
                                print(f"[Patch] ⚠️ Suppressed ValueError for {corrid}: {e}")
                        except Exception as e:
                            print(f"[Patch] ⚠️ Suppressed Exception for {corrid}: {e}")
                    else:
                        # Fallback to default if needed (mostly unused for fetch_contracts)
                        self.onreply_callback(topic, corrid, reply)
            except Exception as e:
                 print(f"[Patch] Critical error in patched_onreply: {e}")
            return 0

        pysolace.SolClient.onreply_callback_wrap = patched_onreply_callback_wrap
        
        print(f"[Patch] pysolace fully monkeypatched (Core Request + OnReply).")
    else:
        print(f"[Patch] pysolace already patched.")

except ImportError:
    print("[Patch] pysolace not found, skipping patch.")
except Exception as e:
    print(f"[Patch] Failed to patch pysolace: {e}")
